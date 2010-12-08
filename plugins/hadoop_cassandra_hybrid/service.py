import os
import sys
import time
import subprocess
import urllib
import tempfile
import socket
import re

from cloud.cluster import TimeoutException
from cloud.service import InstanceTemplate
from cloud.plugin import ServicePlugin 
from cloud.util import xstr
from cloud.util import url_get

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

class HadoopCassandraService(ServicePlugin):
    """
    """
    NAMENODE = "hybrid_nn"
    SECONDARY_NAMENODE = "hybrid_snn"
    JOBTRACKER = "hybrid_jt"
    DATANODE = "hybrid_dn"
    TASKTRACKER = "hybrid_tt"
    CASSANDRA_NODE = "hybrid_cn"
    HADOOP_CASSANDRA_NODE = "hcn"

    def __init__(self):
        super(HadoopCassandraService, self).__init__()

    def get_roles(self):
        return [self.NAMENODE]

    def get_instances(self):
        """
        Return a list of tuples resembling (role_of_instance, instance)
        """
        return self.cluster.get_instances_in_role(self.NAMENODE, "running") + \
               self.cluster.get_instances_in_role(self.DATANODE, "running")

    def launch_cluster(self, instance_templates, client_cidr, config_dir,
                             ssh_options, cassandra_config_file,
                             cassandra_keyspace_file=None):

        number_of_tasktrackers = 0
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

        # cassandra specific instances and setup
        cassandra_instances = self.cluster.get_instances_in_role(self.DATANODE, "running")
        self._transfer_config_files(ssh_options, 
                                    cassandra_config_file, 
                                    cassandra_keyspace_file, 
                                    instances=cassandra_instances)
        self.start_cassandra(ssh_options, 
                             create_keyspaces=(cassandra_keyspace_file is not None), 
                             instances=cassandra_instances)

        return self._get_jobtracker()

    def login(self, instance, ssh_options):
        ssh_command = self._get_standard_ssh_command(instance, ssh_options)
        subprocess.call(ssh_command, shell=True)

    def _sanitize_role_name(self, role):
        """
        Replace characters in role name with ones allowed in bash variable names
        """
        return role.replace('+', '_').upper()
    

    def _get_namenode(self):
        instances = self.cluster.get_instances_in_role(self.NAMENODE, "running")
        if not instances:
          return None
        return instances[0]

    def _get_jobtracker(self):
        instances = self.cluster.get_instances_in_role(self.JOBTRACKER, "running")
        if not instances:
          return None
        return instances[0]
    
    def _create_client_hadoop_site_file(self, config_dir):
        namenode = self._get_namenode()
        jobtracker = self._get_jobtracker()
        cluster_dir = os.path.join(config_dir, ".hadoop", self.cluster.name)
        aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
        aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']

        if not os.path.exists(cluster_dir):
          os.makedirs(cluster_dir)

        params = {
            'namenode': self._get_namenode().public_dns_name,
            'jobtracker': self._get_jobtracker().public_dns_name,
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

        namenode = self._get_namenode()
        jobtracker = self._get_jobtracker()

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
        jobtracker = self._get_jobtracker()
        if not jobtracker:
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
                if (time.time() - start_time >= timeout):
                    raise TimeoutException()
                try:
                    actual_running = self._number_of_tasktrackers(jobtracker.public_dns_name, 5, 2)
                    self.logger.debug("Sleeping for %d seconds..." % wait_time)
                    time.sleep(wait_time)
                    previous_running = actual_running
                except IOError:
                    pass
        
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

        namenode = self._get_namenode()
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

    def _wait_for_cassandra_install(self, instance, ssh_options):
        """
        Simply wait for the cassandra directory to be available so that we can begin configuring
        the service before starting it
        """
        wait_time = 3
        command = "ls /usr/local/apache-cassandra"
        ssh_command = self._get_standard_ssh_command(instance, ssh_options, command)
        self.logger.debug(ssh_command)
        timeout = 600

        start_time = time.time()
        while True:
            if (time.time() - start_time >= timeout):
                raise TimeoutException()
            retcode = subprocess.call(ssh_command, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            if retcode == 0:
                break
            self.logger.debug("Sleeping for %d seconds..." % wait_time)
            time.sleep(wait_time)

    def _transfer_config_files(self, ssh_options, config_file, keyspace_file=None, 
                                     instances=None):
        """
        """
        if instances is None:
            instances = self.get_instances()

        self.logger.debug("Waiting for %d Cassandra instance(s) to install..." % len(instances))
        for instance in instances:
            self._wait_for_cassandra_install(instance, ssh_options)

        self.logger.debug("Copying configuration files to %d Cassandra instances..." % len(instances))

        seed_ips = [str(instance.private_dns_name) for instance in instances[:2]]
        tokens = self._get_evenly_spaced_tokens_for_n_instances(len(instances))

        # for each instance, generate a config file from the original file and upload it to
        # the cluster node
        for i in range(len(instances)):
            local_file, remote_file = self._modify_config_file(instances[i], config_file, seed_ips, str(tokens[i]))

            # Upload modified config file
            scp_command = 'scp %s -r %s root@%s:/usr/local/apache-cassandra/conf/%s' % (xstr(ssh_options),
                                                     local_file, instances[i].public_dns_name, remote_file)
            subprocess.call(scp_command, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)

            # delete temporary file
            os.unlink(local_file)

        if keyspace_file:
            keyspace_data = urllib.urlopen(keyspace_file).read()
            fd, temp_keyspace_file = tempfile.mkstemp(prefix="keyspaces.txt_", text=True)
            os.write(fd, keyspace_data)
            os.close(fd)

            self.logger.debug("Copying keyspace definition file to first Cassandra instance...")

            # Upload keyspace definitions file
            scp_command = 'scp %s -r %s root@%s:/usr/local/apache-cassandra/conf/keyspaces.txt' % \
                          (xstr(ssh_options), temp_keyspace_file, instances[0].public_dns_name)
            subprocess.call(scp_command, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)

            # remove temporary file
            os.unlink(temp_keyspace_file)

    def _modify_config_file(self, instance, config_file, seed_ips, token):
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
            yaml['listen_address'] = str(instance.private_dns_name)
            yaml['rpc_address'] = str(instance.public_dns_name)

            fd, temp_file = tempfile.mkstemp(prefix='cassandra.yaml_', text=True)
            os.write(fd, dump_yaml(yaml))
            os.close(fd)
        else:
            raise Exception("Configuration file must be one of xml or yaml") 

        return temp_file, remote_file
    
    def _get_evenly_spaced_tokens_for_n_instances(self, n):
        return [i*(2**127/n) for i in range(1,n+1)]

    def _create_keyspaces_from_definitions_file(self, instance, ssh_options):
        # TODO: Keyspaces could already exist...how do I check this?
        # TODO: Can it be an arbitrary node?

        self.logger.debug("Creating keyspaces using Thrift API via keyspaces_definitions_file...")

        # test for the keyspace file first
        command = "ls /usr/local/apache-cassandra/conf/keyspaces.txt"
        ssh_command = self._get_standard_ssh_command(instance, ssh_options, command)
        retcode = subprocess.call(ssh_command, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)

        if retcode != 0:
            self.logger.warn("Unable to find /usr/local/apache-cassandra/conf/keyspaces.txt. Skipping keyspace generation.")
            return
        else:
            self.logger.debug("Found keyspaces.txt...Proceeding with keyspace generation.")

        command = "/usr/local/apache-cassandra/bin/cassandra-cli --host %s --batch " \
                  "< /usr/local/apache-cassandra/conf/keyspaces.txt" % instance.private_dns_name
        ssh_command = self._get_standard_ssh_command(instance, ssh_options, command)
        retcode = subprocess.call(ssh_command, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)

        # TODO: do this or not?
        # remove keyspace file
        #command = "rm -rf /usr/local/apache-cassandra/conf/keyspaces.txt"
        #ssh_command = self._get_standard_ssh_command(instance, ssh_options, command)
        #subprocess.call(ssh_command, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)

    def print_ring(self, ssh_options, instance=None):
        if instance is None:
          instance = self.get_instances()[0]

        print "\nRing configuration..."
        print "NOTE: May not be accurate if the cluster just started."
        command = "/usr/local/apache-cassandra/bin/nodetool -h localhost ring"
        ssh_command = self._get_standard_ssh_command(instance, ssh_options, command)
        subprocess.call(ssh_command, shell=True)

    def start_cassandra(self, ssh_options, create_keyspaces=False, instances=None):
        if instances is None:
            instances = self.get_instances()

        self.logger.debug("Starting Cassandra service on %d instance(s)..." % len(instances))

        for instance in instances:
            # if checks to see if cassandra is already running 
            command = "if [ ! -f /root/cassandra.pid ]; then `nohup /usr/local/apache-cassandra/bin/cassandra -p /root/cassandra.pid &> /root/cassandra.out &`; fi"
            ssh_command = self._get_standard_ssh_command(instance, ssh_options, command)
            retcode = subprocess.call(ssh_command, shell=True)

            if retcode != 0:
                self.logger.warn("Return code for starting Cassandra: %d" % retcode)

        # test connection
        self.logger.debug("Testing connection to each Cassandra instance...")

        timeout = 600
        temp_instances = instances[:]
        start_time = time.time()
        while len(temp_instances) > 0:
            if (time.time() - start_time >= timeout):
                raise TimeoutException()
            
            command = "/usr/local/apache-cassandra/bin/nodetool -h %s ring" % temp_instances[-1].private_dns_name
            ssh_command = self._get_standard_ssh_command(temp_instances[-1], ssh_options, command)
            retcode = subprocess.call(ssh_command, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)

            if retcode == 0:
                temp_instances.pop()
            else:
                self.logger.warn("Return code for 'nodetool ring' on '%s': %d" % (temp_instances[-1].id, retcode))

        if create_keyspaces:
            self._create_keyspaces_from_definitions_file(instances[0], ssh_options)
        else:
            self.logger.debug("create_keyspaces is False. Skipping keyspace generation.")
        
        # TODO: Do I need to wait for the keyspaces to propagate before printing the ring?
        # print ring after everything started
        self.print_ring(ssh_options, instances[0])

        self.logger.debug("Startup complete.")

    def stop_cassandra(self, ssh_options, instances=None):
        if instances is None:
            instances = self.get_instances()

        for instance in instances:
            command = "kill `cat /root/cassandra.pid`"
            ssh_command = self._get_standard_ssh_command(instance, ssh_options, command)
            retcode = subprocess.call(ssh_command, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)

    def login(self, instance, ssh_options):
        ssh_command = self._get_standard_ssh_command(instance, ssh_options)
        subprocess.call(ssh_command, shell=True)
