from __future__ import with_statement

import os
import sys
import time
import datetime
import subprocess
import urllib
import tempfile
import socket
import re

from fabric.api import *
from fabric.state import output

from cloud.cluster import TimeoutException
from cloud.service import InstanceTemplate
from cloud.plugin import ServicePlugin 
from cloud.util import xstr
from cloud.util import url_get

# fabric output settings
output.running = False
output.stdout = False
output.stderr = False

class HadoopService(ServicePlugin):
    """
    """
    NAMENODE = "nn"
    SECONDARY_NAMENODE = "snn"
    JOBTRACKER = "jt"
    DATANODE = "dn"
    TASKTRACKER = "tt"

    def __init__(self):
        super(HadoopService, self).__init__()

    def get_roles(self):
        return [self.NAMENODE]

    def get_instances(self):
        """
        Return a list of tuples resembling (role_of_instance, instance)
        """
        return self.cluster.get_instances_in_role(self.NAMENODE, "running") + \
               self.cluster.get_instances_in_role(self.DATANODE, "running")

    def launch_cluster(self, instance_templates, client_cidr, config_dir, num_existing_tasktrackers=0):
        number_of_tasktrackers = num_existing_tasktrackers
        roles = []
        for it in instance_templates:
          roles.extend(it.roles)
          if self.TASKTRACKER in it.roles:
            number_of_tasktrackers += it.number

        singleton_hosts = []
        started_instance_ids = [] 
        expected_instance_count = sum([it.number for it in instance_templates])

        for instance_template in instance_templates:
            self.logger.debug("Launching %d instance(s) with role(s) %s..." % (
                instance_template.number,
                str(instance_template.roles),
            ))
            self.logger.debug("Instance(s) will have extra environment variables: %s" % (
                singleton_hosts,
            ))
            instance_template.add_env_strings(singleton_hosts)
            instance_ids = self._launch_instances(instance_template)

            if instance_template.number == 1:
                if len(instance_ids) != 1:
                    logger.error("Expected a single '%s' instance, but found %s.",
                                 "".join(instance_template.roles), 
                                 len(instance_ids))
                    return False
                else:
                    # wait for the instances to start
                    self.cluster.wait_for_instances(instance_ids)
                    instance = self.get_instances()[0]

                    for role in instance_template.roles:
                        singleton_host_env = "%s_HOST=%s" % (
                            self._sanitize_role_name(role),
                            instance.public_dns_name,
                        )
                        singleton_hosts.append(singleton_host_env)

            started_instance_ids.extend(instance_ids)

        if len(started_instance_ids) != expected_instance_count:
            self.logger.warn("Total number of reported instance ids (%d) " \
                             "does not match total requested number (%d)" % \
                             (len(started_instance_ids), instance_template.number))

        self.logger.debug("Waiting for %s instance(s) to start: %s" % \
            (len(started_instance_ids), ", ".join(started_instance_ids)))
        time.sleep(1)
        
        try:
            self.cluster.wait_for_instances(started_instance_ids)
        except TimeoutException:
            self.logger.error("Timeout while waiting for %d instances to start." % \
                              len(started_instance_ids))

        instances = self.get_instances()

        self.logger.debug("Instances started: %s" % (str(instances),))

        self._create_client_hadoop_site_file(config_dir)
        self._authorize_client_ports(client_cidr)
        self._attach_storage(roles)
        try:
            self._wait_for_hadoop(number_of_tasktrackers)
        except TimeoutException:
            print "Timeout while waiting for Hadoop to start. Please check logs on" + \
                  " cluster."
        return self.get_jobtracker()

    def terminate_nodes(self, nodes, options):
        """Terminate a subset of nodes from a cluster.
        nodes is a list of boto.ec2.instance.Instance objects"""

        exclude_hosts = ""
        for node in nodes:
            print("Terminating instance %s ... " % node.id),
            exclude_hosts += node.private_dns_name + "\n"
            node.terminate()
            print("done")

        print("Removing nodes from hadoop ..."),
        env.host_string = self.get_namenode().public_dns_name
        env.user = "root"
        env.key_filename = options["private_key"]
        hadoop_home = self.get_hadoop_home(env.key_filename)
        run('echo "%s" > %s/conf/exclude' % (exclude_hosts.strip(), hadoop_home))
        fab_output = run("sudo -u hadoop %s/bin/hadoop dfsadmin -refreshNodes" % 
            hadoop_home)
        fab_output = run("sudo -u hadoop %s/bin/hadoop mradmin -refreshNodes" %
            hadoop_home)
        print("done")

    def _extract_dfs(self, dfs_output):
        """Clean up and extract some info from dfsadmin output."""
        
        # trim off the top cruft
        dfs_lines = dfs_output.splitlines()
        for line in dfs_lines:
            if line.startswith("---"):
                break
        datanode_lines = dfs_lines[dfs_lines.index(line)+1:]
        dfs_summary = datanode_lines[0]

        # now pull out info for each node
        nodes = []
        node_info_lines = "\n".join(datanode_lines[2:]).split("\n\n\n")
        for node in node_info_lines:
            node_info = [{line.split(": ")[0].strip().lower():line.split(": ")[1].strip()} 
                for line in node.splitlines()]
            nodes.append(
                {"private_ip": node_info[0]["name"].split(":")[0],
                 "last_contact": time.strptime(
                    node_info[8]["last contact"],"%a %b %d %H:%M:%S %Z %Y")})

        return nodes

    def find_dead_nodes(self, cluster_name, options):
        """Find a list of nodes that are dead."""
        instances = self.get_instances()
        name_nodes = self.cluster.get_instances_in_role(self.NAMENODE, "running")
        if not name_nodes:
            print("No name node found.")
            return False

        env.host_string = name_nodes[0].public_dns_name
        env.user = "root"
        env.key_filename = options["private_key"]
        fab_output = run("sudo -u hadoop %s/bin/hadoop dfsadmin -report" % 
            self.get_hadoop_home(env.key_filename))

        # list of hdfs nodes
        dfs_nodes = self._extract_dfs(fab_output)
        dead_nodes = []
        for node in dfs_nodes:

            # hadoop appears to consider a node dead if it loses the heartbeat
            # for 630 seconds (10.5 minutes)
            time_lapse = (datetime.timedelta(seconds=time.mktime(time.gmtime())) 
                - datetime.timedelta(seconds=time.mktime(node["last_contact"])))
            if time_lapse.seconds > 630:
                for instance in instances:
                    if instance.private_ip_address == node["private_ip"]:
                        dead_nodes.append(instance)
                        break

        return dead_nodes
        
    def login(self, instance, ssh_options):
        ssh_command = self._get_standard_ssh_command(instance, ssh_options)
        subprocess.call(ssh_command, shell=True)

    def _sanitize_role_name(self, role):
        """
        Replace characters in role name with ones allowed in bash variable names
        """
        return role.replace('+', '_').upper()
   
    def get_hadoop_home(self, private_key):
        """Find out what HADOOP_HOME is on the namenode.  You must provide the
        private_key necessary to connect to the namenode."""
        
        if not private_key:
            return None

        env.host_string = self.get_namenode().public_dns_name
        env.user = "root"
        env.key_filename = private_key
        fab_output = run("echo $HADOOP_HOME")
        return fab_output.rstrip() if fab_output else None

    def get_namenode(self):
        instances = self.cluster.get_instances_in_role(self.NAMENODE, "running")
        if not instances:
          return None
        return instances[0]

    def get_jobtracker(self):
        instances = self.cluster.get_instances_in_role(self.JOBTRACKER, "running")
        if not instances:
          return None
        return instances[0]

    def get_datanodes(self):
        instances = self.cluster.get_instances_in_role(self.DATANODE, "running")
        if not instances:
            return None
        return instances

    def get_tasktrackers(self):
        instances = self.cluster.get_instances_in_role(self.TASKTRACKER, "running")
        if not instances:
            return None
        return instances
    
    def _create_client_hadoop_site_file(self, config_dir):
        namenode = self.get_namenode()
        jobtracker = self.get_jobtracker()
        cluster_dir = os.path.join(config_dir, ".hadoop", self.cluster.name)
        aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
        aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']

        if not os.path.exists(cluster_dir):
          os.makedirs(cluster_dir)

        params = {
            'namenode': namenode.public_dns_name,
            'jobtracker': jobtracker.public_dns_name,
            'aws_access_key_id': os.environ['AWS_ACCESS_KEY_ID'],
            'aws_secret_access_key': os.environ['AWS_SECRET_ACCESS_KEY']
        }
        self.logger.debug("hadoop-site.xml params: %s" % str(params))

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
</configuration>""" % params)

    def _authorize_client_ports(self, client_cidrs=[]):
        if not client_cidrs:
            self.logger.debug("No client CIDRs specified, using local address.")
            client_ip = url_get('http://checkip.amazonaws.com/').strip()
            client_cidrs = ("%s/32" % client_ip,)
        self.logger.debug("Client CIDRs: %s", client_cidrs)

        namenode = self.get_namenode()
        jobtracker = self.get_jobtracker()

        for client_cidr in client_cidrs:
            # Allow access to port 80 on namenode from client
            self.cluster.authorize_role(self.NAMENODE, 80, 80, client_cidr)

            # Allow access to jobtracker UI on master from client
            # (so we can see when the cluster is ready)
            self.cluster.authorize_role(self.JOBTRACKER, 50030, 50030, client_cidr)

        # Allow access to namenode and jobtracker via public address from each other
        namenode_ip = socket.gethostbyname(namenode.public_dns_name)
        jobtracker_ip = socket.gethostbyname(jobtracker.public_dns_name)
        self.cluster.authorize_role(self.NAMENODE, 8020, 8020, "%s/32" % namenode_ip)
        self.cluster.authorize_role(self.NAMENODE, 8020, 8020, "%s/32" % jobtracker_ip)
        self.cluster.authorize_role(self.JOBTRACKER, 8021, 8021, "%s/32" % namenode_ip)
        self.cluster.authorize_role(self.JOBTRACKER, 8021, 8021,
                                    "%s/32" % jobtracker_ip)

    def _wait_for_hadoop(self, number, timeout=600):
        wait_time = 3
        start_time = time.time()
        jobtracker = self.get_jobtracker()
        if not jobtracker:
            self.logger.debug("No jobtracker found")
            return

        self.logger.debug("Waiting for jobtracker to start...")
        previous_running = 0
        while True:
            if (time.time() - start_time >= timeout):
                raise TimeoutException()
            try:
                actual_running = self._number_of_tasktrackers(jobtracker.public_dns_name, 1)
                break
            except IOError:
                pass
            self.logger.debug("Sleeping for %d seconds..." % wait_time)
            time.sleep(wait_time)
        if number > 0:
            self.logger.debug("Waiting for %d tasktrackers to start" % number)
            while actual_running < number:
                self.logger.debug("actual_running: %s" % actual_running)
                self.logger.debug("number: %s" % number)
                if (time.time() - start_time >= timeout):
                    raise TimeoutException()
                try:
                    actual_running = self._number_of_tasktrackers(jobtracker.public_dns_name, 5, 2)
                    self.logger.debug("Sleeping for %d seconds..." % wait_time)
                    time.sleep(wait_time)
                    previous_running = actual_running
                except IOError:
                    pass
            self.logger.debug("actual_running = number (%s = %s)" % (actual_running, number))
        
    # The optional ?type=active is a difference between Hadoop 0.18 and 0.20
    _NUMBER_OF_TASK_TRACKERS = re.compile(r'<a href="machines.jsp(?:\?type=active)?">(\d+)</a>')
  
    def _number_of_tasktrackers(self, jt_hostname, timeout, retries=0):
        url = "http://%s:50030/jobtracker.jsp" % jt_hostname
        jt_page = url_get(url, timeout, retries)
        m = self._NUMBER_OF_TASK_TRACKERS.search(jt_page)
        if m:
            return int(m.group(1))
        return 0

    def proxy(self, ssh_options, instances=None):
        if instances is None:
            return None

        namenode = self.get_namenode()
        if namenode is None:
            self.logger.error("No namenode running. Aborting.")
            return None
        
        options = '-o "ConnectTimeout 10" -o "ServerAliveInterval 60" ' \
                  '-N -D 6666'
        process = subprocess.Popen('ssh %s %s root@%s' % (
                xstr(ssh_options), 
                options, 
                namenode.public_dns_name
            ),
            stdin=subprocess.PIPE, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE,
            shell=True)
        
        return process.pid
    
