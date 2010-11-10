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
Classes for running services on a cluster.
"""

from __future__ import with_statement

from cloud.service import Service
from cloud.cluster import get_cluster
from cloud.cluster import InstanceUserData
from cloud.cluster import TimeoutException
from cloud.providers.ec2 import Ec2Storage
from cloud.util import build_env_string
from cloud.util import url_get
from cloud.util import xstr
from prettytable import PrettyTable
from cassandra.service import CassandraService 

import logging
import os
import re
import socket
import subprocess
import sys
import time

logger = logging.getLogger(__name__)

MASTER = "master"  # Deprecated.

NAMENODE = "hybrid_nn"
SECONDARY_NAMENODE = "hybrid_snn"
JOBTRACKER = "hybrid_jt"
DATANODE = "hybrid_dn"
TASKTRACKER = "hybrid_tt"
CASSANDRA_NODE = "hybrid_cn"
HADOOP_CASSANDRA_NODE = "hcn"

class HadoopCassandraHybridService(Service):
  """
  A HDFS and MapReduce service.
  """
  
  def __init__(self, cluster):
    super(HadoopCassandraHybridService, self).__init__(cluster)
    
  def get_service_code(self):
    return "hadoop"

  def get_clusters_for_provider(self, provider, region):
    """
    Find and return clusters that have a running namenode instance
    """
    clusters = get_cluster(provider).get_clusters_with_role(HADOOP_CASSANDRA_NODE, region=region)
    return clusters
    
  def list_all(self, provider):
    """
    Find and print clusters that have a running namenode instance
    """
    clusters = self.get_clusters_for_provider(provider)
    if not clusters:
      print "No running clusters"
    else:
      for cluster in clusters:
        print cluster
    
  def list(self):
    self.cluster.print_status(roles=[NAMENODE, DATANODE])

  def get_running_instances(self):
    return self.cluster.get_instances_in_role(NAMENODE, "running") + \
           self.cluster.get_instances_in_role(DATANODE, "running")

  def launch_master(self, instance_template, config_dir, client_cidr):
    if self.cluster.check_running(NAMENODE, 0) == False:
      return  # don't proceed if another master is running
    self.launch_cluster((instance_template,), config_dir, client_cidr)
  
  def launch_slaves(self, instance_template):
    instances = self.cluster.check_running(NAMENODE, 1)
    if not instances:
      return
    master = instances[0]
    for role in (NAMENODE, SECONDARY_NAMENODE, JOBTRACKER): 
      singleton_host_env = "%s_HOST=%s" % \
              (self._sanitize_role_name(role), master.public_ip)
      instance_template.add_env_strings((singleton_host_env))
    self._launch_instances(instance_template)              
    self._attach_storage(instance_template.roles)
    self._print_master_url()
      
  def launch_cluster(self, instance_templates, config_dir, client_cidr, ssh_options, config_file, keyspace_file=None):
    number_of_tasktrackers = 0
    roles = []
    for it in instance_templates:
      roles.extend(it.roles)
      if TASKTRACKER in it.roles:
        number_of_tasktrackers += it.number
    self._launch_cluster_instances(instance_templates)
    self._create_client_hadoop_site_file(config_dir)
    self._authorize_client_ports(client_cidr)
    self._attach_storage(roles)

    try:
      self._wait_for_hadoop(number_of_tasktrackers)
    except TimeoutException:
      print "Timeout while waiting for Hadoop to start. Please check logs on" +\
        " cluster."

    # cassandra specific instances and setup
    cassandra_instances = self.cluster.get_instances_in_role(DATANODE, "running")
    cs = CassandraService(self.cluster)
    cs._transfer_config_files(ssh_options, config_file, keyspace_file, instances=cassandra_instances)
    cs.start_cassandra(ssh_options, instances=cassandra_instances)

    self._print_master_url()

  def start_cassandra(self, ssh_options, instances=None):
    if instances is None:
        instances = self.cluster.get_instances_in_role(DATANODE, "running")
    cs = CassandraService(self.cluster)
    cs.start_cassandra(ssh_options, instances=instances)

  def stop_cassandra(self, ssh_options, instances=None):
    if instances is None:
        instances = self.cluster.get_instances_in_role(DATANODE, "running")
    cs = CassandraService(self.cluster)
    cs.stop_cassandra(ssh_options, instances=instances)

  def login(self, ssh_options):
    table = PrettyTable()
    table.set_field_names(("", "Role", "Instance Id", "Public IP", "Private IP"))


    master = self.cluster.get_instances_in_role(NAMENODE, "running")
    slaves = self.cluster.get_instances_in_role(DATANODE, "running")
    for instance in master+slaves:
        table.add_row((len(table.rows)+1, instance.role, instance.id, instance.public_ip, instance.private_ip))

    table.printt()

    while True:
        try:
            choice = raw_input("Node to login to [Enter = quit]: ")
            if choice == "":
                sys.exit(0)
            choice = int(choice)
            if choice > 0 and choice <= len(table.rows):
                ssh_command = "ssh %s root@%s" % (xstr(ssh_options), table.rows[choice-1][3])
                subprocess.call(ssh_command, shell=True)
                break
            else:
                print "Not a valid choice. Try again."
        except ValueError:
            print "Not a valid choice. Try again."
        except KeyboardInterrupt:
            print
            sys.exit(0)
    
  """
  def login(self, ssh_options):
    master = self._get_master()
    if not master:
      sys.exit(1)
    subprocess.call('ssh %s root@%s' % \
                    (xstr(ssh_options), master.public_ip),
                    shell=True)
  """
    
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
  
  def update_slaves_file(self, config_dir, ssh_options, private_key):
    instances = self.cluster.check_running(NAMENODE, 1)
    if not instances:
      sys.exit(1)
    master = instances[0]
    slaves = self.cluster.get_instances_in_role(DATANODE, "running")
    cluster_dir = os.path.join(config_dir, self.cluster.name)
    slaves_file = os.path.join(cluster_dir, 'slaves')
    with open(slaves_file, 'w') as f:
      for slave in slaves:
        f.write(slave.public_ip + "\n")
    subprocess.call('scp %s -r %s root@%s:/etc/hadoop/conf' % \
                    (ssh_options, slaves_file, master.public_ip), shell=True)
    # Copy private key
    subprocess.call('scp %s -r %s root@%s:/root/.ssh/id_rsa' % \
                    (ssh_options, private_key, master.public_ip), shell=True)
    for slave in slaves:
      subprocess.call('scp %s -r %s root@%s:/root/.ssh/id_rsa' % \
                      (ssh_options, private_key, slave.public_ip), shell=True)
        
  def _get_master(self):
    # For split namenode/jobtracker, designate the namenode as the master
    return self._get_namenode()

  def _get_namenode(self):
    instances = self.cluster.get_instances_in_role(NAMENODE, "running")
    if not instances:
      return None
    return instances[0]

  def _get_jobtracker(self):
    instances = self.cluster.get_instances_in_role(JOBTRACKER, "running")
    if not instances:
      return None
    return instances[0]

  def _launch_cluster_instances(self, instance_templates):
    singleton_hosts = []
    for instance_template in instance_templates:
      instance_template.add_env_strings(singleton_hosts)
      instances = self._launch_instances(instance_template, exclude_roles=[HADOOP_CASSANDRA_NODE])
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

  def _sanitize_role_name(self, role):
    """Replace characters in role name with ones allowed in bash variable names"""
    return role.replace('+', '_').upper()

  def _authorize_client_ports(self, client_cidrs=[]):
    if not client_cidrs:
      logger.debug("No client CIDRs specified, using local address.")
      client_ip = url_get('http://checkip.amazonaws.com/').strip()
      client_cidrs = ("%s/32" % client_ip,)
    logger.debug("Client CIDRs: %s", client_cidrs)
    namenode = self._get_namenode()
    jobtracker = self._get_jobtracker()
    for client_cidr in client_cidrs:
      # Allow access to port 80 on namenode from client
      self.cluster.authorize_role(NAMENODE, 80, 80, client_cidr)
      # Allow access to jobtracker UI on master from client
      # (so we can see when the cluster is ready)
      self.cluster.authorize_role(JOBTRACKER, 50030, 50030, client_cidr)
    # Allow access to namenode and jobtracker via public address from each other
    namenode_ip = socket.gethostbyname(namenode.public_ip)
    jobtracker_ip = socket.gethostbyname(jobtracker.public_ip)
    self.cluster.authorize_role(NAMENODE, 8020, 8020, "%s/32" % namenode_ip)
    self.cluster.authorize_role(NAMENODE, 8020, 8020, "%s/32" % jobtracker_ip)
    self.cluster.authorize_role(JOBTRACKER, 8021, 8021, "%s/32" % namenode_ip)
    self.cluster.authorize_role(JOBTRACKER, 8021, 8021,
                                "%s/32" % jobtracker_ip)
  
  def _create_client_hadoop_site_file(self, config_dir):
    namenode = self._get_namenode()
    jobtracker = self._get_jobtracker()
    cluster_dir = os.path.join(config_dir, ".hadoop", self.cluster.name)
    aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    if not os.path.exists(cluster_dir):
      os.makedirs(cluster_dir)
    with open(os.path.join(cluster_dir, 'hadoop-site.xml'), 'w') as f:
      f.write("""<?xml version="1.0"?>
  <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
  <!-- Put site-specific property overrides in this file. -->
  <configuration>
  <property>
    <name>hadoop.job.ugi</name>
    <value>root,root</value>
  </property>
  <property>
    <name>fs.default.name</name>
    <value>hdfs://%(namenode)s:8020/</value>
  </property>
  <property>
    <name>mapred.job.tracker</name>
    <value>%(jobtracker)s:8021</value>
  </property>
  <property>
    <name>hadoop.socks.server</name>
    <value>localhost:6666</value>
  </property>
  <property>
    <name>hadoop.rpc.socket.factory.class.default</name>
    <value>org.apache.hadoop.net.SocksSocketFactory</value>
  </property>
  <property>
    <name>fs.s3.awsAccessKeyId</name>
    <value>%(aws_access_key_id)s</value>
  </property>
  <property>
    <name>fs.s3.awsSecretAccessKey</name>
    <value>%(aws_secret_access_key)s</value>
  </property>
  <property>
    <name>fs.s3n.awsAccessKeyId</name>
    <value>%(aws_access_key_id)s</value>
  </property>
  <property>
    <name>fs.s3n.awsSecretAccessKey</name>
    <value>%(aws_secret_access_key)s</value>
  </property>
  </configuration>
  """ % {'namenode': namenode.public_ip,
    'jobtracker': jobtracker.public_ip,
    'aws_access_key_id': aws_access_key_id,
    'aws_secret_access_key': aws_secret_access_key})        

  def _wait_for_hadoop(self, number, timeout=600):
    start_time = time.time()
    jobtracker = self._get_jobtracker()
    if not jobtracker:
      return
    print "Waiting for jobtracker to start"
    previous_running = 0
    while True:
      if (time.time() - start_time >= timeout):
        raise TimeoutException()
      try:
        actual_running = self._number_of_tasktrackers(jobtracker.public_ip, 1)
        break
      except IOError:
        pass
      sys.stdout.write(".")
      sys.stdout.flush()
      time.sleep(1)
    print
    if number > 0:
      print "Waiting for %d tasktrackers to start" % number
      while actual_running < number:
        if (time.time() - start_time >= timeout):
          raise TimeoutException()
        try:
          actual_running = self._number_of_tasktrackers(jobtracker.public_ip, 5, 2)
          if actual_running != previous_running:
            sys.stdout.write("%d" % actual_running)
          sys.stdout.write(".")
          sys.stdout.flush()
          time.sleep(1)
          previous_running = actual_running
        except IOError:
          pass
      print

  # The optional ?type=active is a difference between Hadoop 0.18 and 0.20
  _NUMBER_OF_TASK_TRACKERS = re.compile(
    r'<a href="machines.jsp(?:\?type=active)?">(\d+)</a>')
  
  def _number_of_tasktrackers(self, jt_hostname, timeout, retries=0):
    jt_page = url_get("http://%s:50030/jobtracker.jsp" % jt_hostname, timeout,
                      retries)
    m = self._NUMBER_OF_TASK_TRACKERS.search(jt_page)
    if m:
      return int(m.group(1))
    return 0

  def _print_master_url(self):
    webserver = self._get_jobtracker()
    if not webserver:
      return
    print "Browse the cluster at http://%s/" % webserver.public_ip

  def _attach_storage(self, roles):
    storage = self.cluster.get_storage()
    if storage.has_any_storage(roles):
      print "Waiting 10 seconds before attaching storage"
      time.sleep(10)
      for role in roles:
        storage.attach(role, self.cluster.get_instances_in_role(role, 'running'))
      storage.print_status(roles)


class ZooKeeperService(Service):
  """
  A ZooKeeper service.
  """

  ZOOKEEPER_ROLE = "zk"

  def __init__(self, cluster):
    super(ZooKeeperService, self).__init__(cluster)
    
  def get_service_code(self):
    return "zookeeper"

  def launch_cluster(self, instance_templates, config_dir, client_cidr):
    self._launch_cluster_instances(instance_templates)
    self._authorize_client_ports(client_cidr)
    self._update_cluster_membership(instance_templates[0].public_key)
    
  def _launch_cluster_instances(self, instance_templates):
    for instance_template in instance_templates:
      instances = self._launch_instances(instance_template)

  def _authorize_client_ports(self, client_cidrs=[]):
    if not client_cidrs:
      logger.debug("No client CIDRs specified, using local address.")
      client_ip = url_get('http://checkip.amazonaws.com/').strip()
      client_cidrs = ("%s/32" % client_ip,)
    logger.debug("Client CIDRs: %s", client_cidrs)
    for client_cidr in client_cidrs:
      self.cluster.authorize_role(self.ZOOKEEPER_ROLE, 2181, 2181, client_cidr)
  
  def _update_cluster_membership(self, public_key):
    time.sleep(30) # wait for SSH daemon to start
    
    ssh_options = '-o StrictHostKeyChecking=no'
    private_key = public_key[:-4] # TODO: pass in private key explicitly

    instances = self.cluster.get_instances_in_role(self.ZOOKEEPER_ROLE,
                                                   'running')
    config_file = 'zoo.cfg'
    with open(config_file, 'w') as f:
      f.write("""# The number of milliseconds of each tick
tickTime=2000
# The number of ticks that the initial
# synchronization phase can take
initLimit=10
# The number of ticks that can pass between
# sending a request and getting an acknowledgement
syncLimit=5
# The directory where the snapshot is stored.
dataDir=/var/log/zookeeper/txlog
# The port at which the clients will connect
clientPort=2181
# The servers in the ensemble
""")
      counter = 1
      for i in instances:
        f.write("server.%s=%s:2888:3888\n" % (counter, i.private_ip))
        counter += 1
    # copy to each node in the cluster
    myid_file = 'myid'
    counter = 1
    for i in instances:
      self._call('scp -i %s %s %s root@%s:/etc/zookeeper/conf/zoo.cfg' \
                 % (private_key, ssh_options, config_file, i.public_ip))
      with open(myid_file, 'w') as f:
        f.write(str(counter) + "\n")
      self._call('scp -i %s %s %s root@%s:/var/log/zookeeper/txlog/myid' \
                 % (private_key, ssh_options, myid_file, i.public_ip))
      counter += 1
    os.remove(config_file)
    os.remove(myid_file)

    # start the zookeeper servers
    for i in instances:
      self._call('ssh -i %s %s root@%s nohup /etc/rc.local &' \
                 % (private_key, ssh_options, i.public_ip))
      
    hosts_string = ",".join(["%s:2181" % i.public_ip for i in instances]) 
    print "ZooKeeper cluster: %s" % hosts_string
