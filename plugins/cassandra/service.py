import os
import sys
import time
import subprocess
import urllib
import tempfile

from cloud.cluster import TimeoutException
from cloud.service import InstanceTemplate
from cloud.plugin import ServicePlugin 
from cloud.util import xstr

from yaml import load as parse_yaml
from yaml import dump as dump_yaml

try:
    from cElementTree import parse as parse_xml
    from cElementTree import tostring as dump_xml
    from cElementTree import Element
except:
    try:
        from xml.etree.cElementTree import parse as parse_xml
        from xml.etree.cElementTree import tostring as dump_xml
        from xml.etree.cElementTree import Element
    except:
        print "*"*80
        print "WARNING: cElementTree module does not exist. Defaulting to elementtree instead."
        print "It's recommended that you install the cElementTree module for faster XML parsing."
        print "*"*80
        from elementtree.ElementTree import parse as parse_xml
        from elementtree.ElementTree import parse as parse_xml
        from elementtree.ElementTree import Element


class CassandraService(ServicePlugin):
    """
    """
    CASSANDRA_NODE = "cn"

    def __init__(self):
        super(CassandraService, self).__init__()

    def get_roles(self):
        return [self.CASSANDRA_NODE]

    def get_instances(self):
        return self.cluster.get_instances_in_role(self.CASSANDRA_NODE, "running")

    def launch_cluster(self, instance_template, ssh_options, config_file, 
                             keyspace_file=None):
        """
        """
        instance_ids = self._launch_instances(instance_template) 

        if len(instance_ids) != instance_template.number:
            self.logger.warn("Number of reported instance ids (%d) " \
                             "does not match requested number (%d)" % \
                             (len(instance_ids), instance_template.number))
        self.logger.debug("Waiting for %s instance(s) to start: %s" % \
            (instance_template.number, ", ".join(instance_ids)))
        time.sleep(1)

        try:
            self.cluster.wait_for_instances(instance_ids)
            self.logger.debug("%d instances started" % (instance_template.number,))
        except TimeoutException:
            self.logger.error("Timeout while waiting for %s instance to start." % \
                ",".join(instance_template.roles))

        instances = self.get_instances()
        self.logger.debug("We have %d current instances...", len(instances))
        new_instances = [instance for instance in instances if instance.id in instance_ids]
        if(len(new_instances) != len(instance_ids)) :
            raise Exception("Could only find %d new instances, expected %s" % (len(new_instances), str(instance_ids)))

        self.logger.debug("Instances started: %s" % (str(new_instances),))
        
        self._attach_storage(instance_template.roles)
        new_cluster = (len(instances) == len(instance_ids))
        self._transfer_config_files(ssh_options,
                                    config_file, 
                                    new_instances,
                                    keyspace_file,
                                    new_cluster=new_cluster)
        self.start_cassandra(ssh_options, config_file, create_keyspaces=(new_cluster and keyspace_file is not None), instances=new_instances)
    
    def _transfer_config_files(self, ssh_options, config_file, instances,
                                     keyspace_file=None, new_cluster=True):
        """
        """
        # we need all instances for seeds, but we should only transfer to new instances!
        all_instances = self.get_instances()
        if new_cluster :
            potential_seeds = all_instances
        else :
            potential_seeds = [instance for instance in all_instances if instance not in instances]


        self.logger.debug("Set tokens: %s" % new_cluster)

        self.logger.debug("Waiting for %d Cassandra instance(s) to install..." % len(instances))
        for instance in instances:
            self._wait_for_cassandra_install(instance, ssh_options)

        self.logger.debug("Copying configuration files to %d Cassandra instances..." % len(instances))

        seed_ips = [str(instance.private_dns_name) for instance in potential_seeds[:2]]
        tokens = self._get_evenly_spaced_tokens_for_n_instances(len(instances))

        # for each instance, generate a config file from the original file and upload it to
        # the cluster node
        for i in range(len(instances)):
            local_file, remote_file = self._modify_config_file(instances[i], config_file, seed_ips, str(tokens[i]), new_cluster)

            # Upload modified config file
            scp_command = 'scp %s -r %s root@%s:/usr/local/apache-cassandra/conf/%s' % (xstr(ssh_options),
                                                     local_file, instances[i].public_dns_name, remote_file)
            subprocess.call(scp_command, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)

            # delete temporary file
            os.unlink(local_file)

        if keyspace_file and new_cluster:
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
    
    def _wait_for_cassandra_install(self, instance, ssh_options):
        """
        Simply wait for the cassandra directory to be available so that we can begin configuring
        the service before starting it
        """
        wait_time = 3
        command = "ls /usr/local/apache-cassandra"
        ssh_command = self._get_standard_ssh_command(instance, ssh_options, command)

        while True:
            retcode = subprocess.call(ssh_command, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            if retcode == 0:
                break
            self.logger.debug("Sleeping for %d seconds..." % wait_time)
            time.sleep(wait_time)

    def _modify_config_file(self, instance, config_file, seed_ips, token, set_tokens):
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
            if set_tokens is True :
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
            if set_tokens is True :
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

    def _get_config_value(self, config_file, yaml_name, xml_name):
        if config_file.endswith(".xml") :
            xml = parse_xml(urllib.urlopen(config_file)).getroot()
            return xml.find(xml_name).text
        elif config_file.endswith(".yaml") :
            yaml = parse_yaml(urllib.urlopen(config_file))
            return yaml[yaml_name]
        else:
            raise Exception("Configuration file must be on of xml or yaml")

    def _create_keyspaces_from_definitions_file(self, instance, config_file, ssh_options):
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

        port = self._get_config_value(config_file, "rpc_port", "ThriftPort")

        command = "/usr/local/apache-cassandra/bin/cassandra-cli --host %s --port %s --batch " \
                  "< /usr/local/apache-cassandra/conf/keyspaces.txt" % (instance.private_dns_name, port)
        ssh_command = self._get_standard_ssh_command(instance, ssh_options, command)
        retcode = subprocess.call(ssh_command, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)

        # TODO: do this or not?
        # remove keyspace file
        #command = "rm -rf /usr/local/apache-cassandra/conf/keyspaces.txt"
        #ssh_command = self._get_standard_ssh_command(instance, ssh_options, command)
        #subprocess.call(ssh_command, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)

    def print_ring(self, ssh_options, instance=None):
        print "\nRing configuration..."
        print "NOTE: May not be accurate if the cluster just started."
        return self._run_nodetool(ssh_options, "ring", instance)

    def _run_nodetool(self, ssh_options, ntcommand, instance=None):
        if instance is None:
          instance = self.get_instances()[0]

        self.logger.debug("running nodetool on instance %s", instance.id)
        command = "/usr/local/apache-cassandra/bin/nodetool -h localhost %s" % ntcommand
        ssh_command = self._get_standard_ssh_command(instance, ssh_options, command)
        subprocess.call(ssh_command, shell=True)

    def rebalance(self, ssh_options):
        instances = self.get_instances()
        tokens = self._get_evenly_spaced_tokens_for_n_instances(len(instances))
        self.logger.info("new token space: %s" % str(tokens))
        for i in range(len(instances)) :
            token = tokens[i]
            instance = instances[i]
            self.logger.info("Moving instance %s to token %s" % (instance.id, token))
            retcode = self._run_nodetool(ssh_options, "move %s" % token, instance=instance)
            if retcode != 0 :
                self.logger.warn("Move failed for instance %s with return code %d..." % (instance.id, retcode))
            else :
                self.logger.info("Move succeeded for instance %s..." % instance.id)


    def start_cassandra(self, ssh_options, config_file, create_keyspaces=False, instances=None):
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
            self._create_keyspaces_from_definitions_file(instances[0], config_file, ssh_options)
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
