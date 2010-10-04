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
  
  def _transfer_storage_conf_files(self, ssh_options, storage_conf_file, instances=None):

    # skip the transfer of the storage conf file because the user chose not
    # to supply one in the configuration. the user has already been warned
    # and has explicitly chosen to continue without one.
    if storage_conf_file is None:
        return

    if instances is None:
        instances = self._get_node_instances()

    print "Waiting for %d Cassandra instances to install..." % len(instances)
    for instance in instances:
        self._wait_for_cassandra_install(instance, ssh_options)
    print ""

    print "Copying storage-conf.xml files to %d Cassandra instances..." % len(instances)

    # every node will have the same seeds
    seeds = "".join(["<Seed>%s</Seed>" % (instance.private_ip,) for instance in instances[:2]])
    seeds_replacement = "<Seeds>%s</Seeds>" % seeds

    # each node will have a different initial token calculated
    initial_tokens = self._get_evenly_spaced_tokens_for_n_instances(len(instances))
    initial_token_replacement = "<InitialToken>%d</InitialToken>"

    for i in range(len(instances)):
        replacements = { 
            "%SEEDS%": seeds_replacement,
            "%INITIAL_TOKEN%": initial_token_replacement % initial_tokens[i]
        }
        data = InstanceUserData(storage_conf_file, replacements)
        fd, fp = tempfile.mkstemp(prefix="storage-conf.xml_", text=True)
        os.write(fd, data.read())
        os.close(fd)
        scp_command = 'scp %s -r %s root@%s:/usr/local/apache-cassandra/conf/storage-conf.xml' % (xstr(ssh_options),
                                                 fp, instances[i].public_ip)

        subprocess.call(scp_command, shell=True)
        os.unlink(fp)

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

    print "Starting Cassandra service on %d instance(s)..." % len(instances)

    for instance in instances:
        command = "nohup /usr/local/apache-cassandra/bin/cassandra -p /root/cassandra.pid &> /root/cassandra.out &"
        ssh_command = self._get_standard_ssh_command(instance, ssh_options, command)
        subprocess.call(ssh_command, shell=True)
    
    time.sleep(len(instances)*3)
    self.print_ring(ssh_options, instances[0])

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

    print "Ring configuration..."
    command = "/usr/local/apache-cassandra/bin/nodetool -h localhost ring"
    ssh_command = self._get_standard_ssh_command(instance, ssh_options, command)
    subprocess.call(ssh_command, shell=True)
    
  """
  NOTE: Only a single instance template for CassandraService
  """
  def launch_cluster(self, instance_template, config_dir, client_cidr, ssh_options, storage_conf_file):
    roles = instance_template.roles

    instances = self._launch_cluster_instances(instance_template)
    self._attach_storage(roles)
    self._transfer_storage_conf_files(ssh_options, storage_conf_file, instances=instances)
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
