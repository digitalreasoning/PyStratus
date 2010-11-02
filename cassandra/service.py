# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Implementation for a Cassandra Service object
"""

from __future__ import with_statement

from prettytable import PrettyTable
from cloud.service import Service
from cloud.cluster import get_cluster
from cloud.cluster import InstanceUserData
from cloud.cluster import TimeoutException
from cloud.providers.ec2 import Ec2Storage
from cloud.util import build_env_string
from cloud.util import url_get
from cloud.util import xstr
import logging
import types
import os
import re
import socket
import subprocess
import sys
import time
import tempfile
import urllib
import urlparse

from yaml import load as parse_yaml
from yaml import dump as dump_yaml

try:
    from cElementTree import parse as parse_xml
    from cElementTree import tostring as dump_xml
    from cElementTree import Element
except:
    print "*"*80
    print "WARNING: cElementTree module does not exist. Defaulting to elementtree instead."
    print "It's recommended that you install the cElementTree module for faster XML parsing."
    print "*"*80
    from elementtree.ElementTree import parse as parse_xml
    from elementtree.ElementTree import parse as parse_xml
    from elementtree.ElementTree import Element

logger = logging.getLogger(__name__)

# Role code to use for cassandra services
CASSANDRA_NODE = "cn"

class CassandraService(Service):
  """
  A Cassandra service
  """
  
  def __init__(self, cluster):
    super(CassandraService, self).__init__(cluster)
    
  def get_service_code(self):
    return "cassandra"

  def get_clusters_for_provider(self, provider):
    """
    Find and return clusters that have running a cassandra instance
    """
    return get_cluster(provider).get_clusters_with_role(CASSANDRA_NODE)
    
  def list_all(self, provider):
    """
    Find and print clusters that have a running cassandra instance
    """
    clusters = self.get_clusters_for_provider(provider)

    if not clusters:
      print "No running Cassandra clusters"
    else:
      for cluster in clusters:
        print cluster
    
  def list(self):
    self.cluster.print_status(roles=[CASSANDRA_NODE])

  def get_running_instances(self):
    return self.cluster.get_instances_in_role(CASSANDRA_NODE, "running")

  def _modify_config_file(self, config_file, seed_ips, token):
    # XML (0.6.x) 
    if config_file.endswith(".xml"):
        remote_file = "storage-conf.xml"

        xml = parse_xml(urllib.urlopen(config_file)).getroot()

        #  Seeds
        seeds = xml.find("Seeds")
        if seeds is not None:
            while seeds.getchildren():
                seeds.remove(seeds.getchildren()[0])
        else:
            seeds = Element("Seeds")
            xml.append(seeds)

        for seed_ip in seed_ips:
            seed = Element("Seed")
            seed.text = seed_ip
            seeds.append(seed)

        # Initial token
        initial_token = xml.find("InitialToken")
        if initial_token is None:
            initial_token = Element("InitialToken")
            xml.append(initial_token)
        initial_token.text = token

        # Logs
        commit_log_directory = xml.find("CommitLogDirectory")
        if commit_log_directory is None:
            commit_log_directory = Element("CommitLogDirectory")
            xml.append(commit_log_directory)
        commit_log_directory.text = "/mnt/cassandra-logs"

        # Data 
        data_file_directories = xml.find("DataFileDirectories")
        if data_file_directories is not None:
            while data_file_directories.getchildren():
                data_file_directories.remove(data_file_directories.getchildren()[0])
        else:
            data_file_directories = Element("DataFileDirectories")
            xml.append(data_file_directories)
        data_file_directory = Element("DataFileDirectory")
        data_file_directory.text = "/mnt/cassandra-data"
        data_file_directories.append(data_file_directory)


        # listen address
        listen_address = xml.find("ListenAddress")
        if listen_address is None:
            listen_address = Element("ListenAddress")
            xml.append(listen_address)
        listen_address.text = ""

        # thrift address
        thrift_address = xml.find("ThriftAddress")
        if thrift_address is None:
            thrift_address = Element("ThriftAddress")
            xml.append(thrift_address)
        thrift_address.text = ""

        fd, temp_file = tempfile.mkstemp(prefix='storage-conf.xml_', text=True)
        os.write(fd, dump_xml(xml))
        os.close(fd)
        
    # YAML (0.7.x)
    elif config_file.endswith(".yaml"):
        remote_file = "cassandra.yaml"

        yaml = parse_yaml(urllib.urlopen(config_file))
        yaml['seeds'] = seed_ips
        yaml['initial_token'] = token
        yaml['data_file_directories'] = ['/mnt/cassandra-data']
        yaml['commitlog_directory'] = '/mnt/cassandra-logs'
        yaml['listen_address'] = None
        yaml['rpc_address'] = None

        fd, temp_file = tempfile.mkstemp(prefix='cassandra.yaml_', text=True)
        os.write(fd, dump_yaml(yaml))
        os.close(fd)
    else:
        raise Exception("Configuration file must be one of xml or yaml") 

    return temp_file, remote_file
  
  def _transfer_config_files(self, ssh_options, config_file, keyspace_file=None, instances=None):

    if instances is None:
        instances = self._get_node_instances()

    print "Waiting for %d Cassandra instance(s) to install..." % len(instances)
    for instance in instances:
        self._wait_for_cassandra_install(instance, ssh_options)
    print ""

    print "Copying configuration files to %d Cassandra instances..." % len(instances)

    seed_ips = [str(instance.private_ip) for instance in instances[:2]]
    tokens = self._get_evenly_spaced_tokens_for_n_instances(len(instances))

    # for each instance, generate a config file from the original file and upload it to
    # the cluster node
    for i in range(len(instances)):
        local_file, remote_file = self._modify_config_file(config_file, seed_ips, str(tokens[i]))

        # Upload modified config file
        scp_command = 'scp %s -r %s root@%s:/usr/local/apache-cassandra/conf/%s' % (xstr(ssh_options),
                                                 local_file, instances[i].public_ip, remote_file)
        subprocess.call(scp_command, shell=True)

        # delete temporary file
        os.unlink(local_file)

    if keyspace_file:
        keyspace_data = urllib.urlopen(keyspace_file).read()
        fd, temp_keyspace_file = tempfile.mkstemp(prefix="keyspaces.txt_", text=True)
        os.write(fd, keyspace_data)
        os.close(fd)

        print "\nCopying keyspace definition file to 1 Cassandra instance..."

        # Upload keyspace definitions file
        scp_command = 'scp %s -r %s root@%s:/usr/local/apache-cassandra/conf/keyspaces.txt' % (xstr(ssh_options),
                                                                           temp_keyspace_file, instances[0].public_ip)
        subprocess.call(scp_command, shell=True)

        os.unlink(temp_keyspace_file)


  def _get_evenly_spaced_tokens_for_n_instances(self, n):
    return [i*(2**127/n) for i in range(1,n+1)]

  def _get_standard_ssh_command(self, instance, ssh_options, remote_command):
    return "ssh %s root@%s '%s'" % (xstr(ssh_options), instance.public_ip, remote_command)

  def _wait_for_cassandra_install(self, instance, ssh_options):
    """
    Simply wait for the cassandra directory to be available so that we can begin configuring
    the service before starting it
    """
    command = "ls /usr/local/apache-cassandra"
    ssh_command = self._get_standard_ssh_command(instance, ssh_options, command)

    while True:
        retcode = subprocess.call(ssh_command, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        if retcode == 0:
            break
        sys.stdout.write(".")
        sys.stdout.flush()
        time.sleep(1)

  def start_cassandra(self, ssh_options, instances=None):
    if instances is None:
        instances = self._get_node_instances()

    print "\nStarting Cassandra service on %d instance(s)..." % len(instances)

    for instance in instances:
        command = "nohup /usr/local/apache-cassandra/bin/cassandra -p /root/cassandra.pid &> /root/cassandra.out &"
        ssh_command = self._get_standard_ssh_command(instance, ssh_options, command)
        subprocess.call(ssh_command, shell=True)

    # test connection
    timeout = 0
    i = 0
    while i < len(instances):
        command = "/usr/local/apache-cassandra/bin/nodetool -h %s info" % instances[i].private_ip
        ssh_command = self._get_standard_ssh_command(instances[i], ssh_options, command)

        while True:
            retcode = subprocess.call(ssh_command, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            if retcode == 0:
                i += 1
                break
            sys.stdout.write(".")
            sys.stdout.flush()
            timeout += 1

            # TODO: Set constant somewhere for timeouts?
            if timeout >= 5:
                raise Exception("Timeout occurred while waiting for Cassandra services to start...")

    self._create_keyspaces_from_definitions_file(instances[0], ssh_options)
    
    # TODO: Do I need to wait for the keyspaces to propagate before printing the ring?
    # print ring after everything started
    self.print_ring(ssh_options, instances[0])

  def _create_keyspaces_from_definitions_file(self, instance, ssh_options):
    # TODO: Keyspaces could already exist...how do I check this?
    # TODO: Can it be an arbitrary node?

    # test for the keyspace file first
    command = "ls /usr/local/apache-cassandra/conf/keyspaces.txt"
    ssh_command = self._get_standard_ssh_command(instance, ssh_options, command)
    retcode = subprocess.call(ssh_command, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)

    if retcode != 0:
        # no keyspace definition file 
        return

    print "Creating keyspaces using Thrift API via keyspaces_definitions_file..."

    command = "/usr/local/apache-cassandra/bin/cassandra-cli --host %s --batch < /usr/local/apache-cassandra/conf/keyspaces.txt" % instance.private_ip
    ssh_command = self._get_standard_ssh_command(instance, ssh_options, command)
    retcode = subprocess.call(ssh_command, shell=True)#stderr=subprocess.PIPE, stdout=subprocess.PIPE)

    # remove keyspace file
    command = "rm -rf /usr/local/apache-cassandra/conf/keyspaces.txt"
    ssh_command = self._get_standard_ssh_command(instance, ssh_options, command)
    subprocess.call(ssh_command, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)

    

  def stop_cassandra(self, ssh_options, instances=None):
    if instances is None:
        instances = self._get_node_instances()

    print "Stopping Cassandra service on %d instance(s)..." % len(instances)

    for instance in instances:
        command = "kill `cat /root/cassandra.pid`"
        ssh_command = self._get_standard_ssh_command(instance, ssh_options, command)
        subprocess.call(ssh_command, shell=True)

  def print_ring(self, ssh_options, instance=None):
    if instance is None:
      instance = self.get_running_instances()[0]

    print "\nRing configuration..."
    command = "/usr/local/apache-cassandra/bin/nodetool -h localhost ring"
    ssh_command = self._get_standard_ssh_command(instance, ssh_options, command)
    subprocess.call(ssh_command, shell=True)

  """
  NOTE: Only a single instance template for CassandraService
  """
  def launch_cluster(self, instance_template, config_dir, client_cidr, ssh_options, config_file, keyspace_file=None):
    roles = instance_template.roles

    instances = self._launch_cluster_instances(instance_template)
    self._attach_storage(roles)
    self._transfer_config_files(ssh_options, config_file, keyspace_file, instances=instances)
    self.start_cassandra(ssh_options, instances=instances)

  def login(self, ssh_options):
    table = PrettyTable()
    table.set_field_names(("", "INSTANCE ID", "PUBLIC IP", "PRIVATE IP"))

    for instance in self._get_node_instances():
        table.add_row((len(table.rows)+1, instance.id, instance.public_ip, instance.private_ip))

    table.printt()

    while True:
        try:
            choice = raw_input("Cassandra node to login to [Enter = quit]: ")
            if choice == "":
                sys.exit(0)
            choice = int(choice)
            if choice > 0 and choice <= len(table.rows):
                ssh_command = "ssh %s root@%s" % (xstr(ssh_options), table.rows[choice-1][2])
                subprocess.call(ssh_command, shell=True)
                break
            else:
                print "Not a valid choice. Try again."
        except ValueError:
            print "Not a valid choice. Try again."
    
  def proxy(self, ssh_options):
    master = self._get_master()
    if not master:
      sys.exit(1)
    options = '-o "ConnectTimeout 10" -o "ServerAliveInterval 60" ' \
              '-N -D 6666'
    process = subprocess.Popen('ssh %s %s root@%s' %
      (xstr(ssh_options), options, master.public_ip),
      stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
      shell=True)
    print """export HADOOP_CLOUD_PROXY_PID=%s;
echo Proxy pid %s;""" % (process.pid, process.pid)
    
  def push(self, ssh_options, file):
    master = self._get_master()
    if not master:
      sys.exit(1)
    subprocess.call('scp %s -r %s root@%s:' % (xstr(ssh_options),
                                               file, master.public_ip),
                                               shell=True)
    
  def execute(self, ssh_options, args):
    master = self._get_master()
    if not master:
      sys.exit(1)
    subprocess.call("ssh %s root@%s '%s'" % (xstr(ssh_options),
                                             master.public_ip,
                                             " ".join(args)), shell=True)
  
  def _get_node_instances(self):
    instances = self.cluster.get_instances_in_role(CASSANDRA_NODE, "running")
    if not instances:
      return None
    return instances

  def _launch_cluster_instances(self, instance_template):
    singleton_hosts = []

    instance_template.add_env_strings(singleton_hosts)
    instances = self._launch_instances(instance_template)
    if instance_template.number == 1:
        if len(instances) != 1:
            logger.error("Expected a single '%s' instance, but found %s.",
                       "".join(instance_template.roles), len(instances))
            return
        else:
            for role in instance_template.roles:
                singleton_host_env = "%s_HOST=%s" % \
                    (self._sanitize_role_name(role),
                    instances[0].public_ip)
                singleton_hosts.append(singleton_host_env)
    return instances

  def _sanitize_role_name(self, role):
    """Replace characters in role name with ones allowed in bash variable names"""
    return role.replace('+', '_').upper()

  def _attach_storage(self, roles):
    storage = self.cluster.get_storage()
    if storage.has_any_storage(roles):
      print "Waiting 10 seconds before attaching storage"
      time.sleep(10)
      for role in roles:
        storage.attach(role, self.cluster.get_instances_in_role(role, 'running'))
      storage.print_status(roles)
