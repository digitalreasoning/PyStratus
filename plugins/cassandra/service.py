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

def find_new_token(existing_tokens):
    range = max(zip([(existing_tokens[-1] - 2**127)] + existing_tokens[:-1], existing_tokens[:]), key=lambda x: x[1] - x[0])
    return range[0] + (range[1]-range[0])/2

def parse_nodeline(nodeline) :
    ip = nodeline[0:15].strip()
    status = nodeline[16:22].strip()
    state = nodeline[23:30].strip()
    load = nodeline[31:46].strip()
    token = long(nodeline[55:-1].strip())
    return {'ip':ip, 'status':status, 'state':state, 'load':load, 'token':token}

class CassandraService(ServicePlugin):
    """
    """
    CASSANDRA_NODE = "cn"
    MAX_RESTART_ATTEMPTS = 3
    current_attempt = 1

    def __init__(self):
        super(CassandraService, self).__init__()

    def get_roles(self):
        return [self.CASSANDRA_NODE]

    def get_instances(self):
        return self.cluster.get_instances_in_role(self.CASSANDRA_NODE, "running")

    def _get_new_tokens_for_n_instances(self, existing_tokens, n):
        all_tokens = existing_tokens[:]
        for i in range(0, n):
            all_tokens.sort()
            new_token = find_new_token(all_tokens)
            all_tokens.append(new_token)
        return [token for token in all_tokens if token not in existing_tokens]

    def expand_cluster(self, instance_template, ssh_options, config_file, new_tokens=None):
        instances = self.get_instances()
        if instance_template.number > len(instances):
            raise Exception("The best we can do is double the cluster size at one time.  Please specify %d instances or less." % len(instances))
        if new_tokens is None:
            existing_tokens = [node['token'] for node in self._discover_ring(ssh_options)]
            self.logger.debug("Tokens: %s" % str(existing_tokens))
            if len(instances) != len(existing_tokens):
                raise Exception("There are %d existing instances, we need that many existing tokens..." % len(instances))
            new_tokens = self._get_new_tokens_for_n_instances([int(token) for token in existing_tokens], instance_template.number)
        elif len(new_tokens) != instance_template.number:
            raise Exception("We are creating %d new instances, we need that many new tokens..." % instance_template.number)

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

        self.logger.info("Instances started: %s" % (str(new_instances),))

        self._attach_storage(instance_template.roles)

        self._transfer_config_files(ssh_options,
                                    config_file,
                                    new_instances,
                                    new_cluster=False,
                                    tokens=new_tokens)
        first = True
        for instance in new_instances:
            if not first:
                self.logger.info("Waiting 2 minutes before starting the next instance...")
                time.sleep(2*60)
            else:
                first = False
            self.logger.info("Starting cassandra on instance %s." % instance.id)
            self.start_cassandra(ssh_options, config_file, create_keyspaces=False, instances=[instance], print_ring=False)
        self.print_ring(ssh_options, instances[0])


    def launch_cluster(self, instance_template, ssh_options, config_file, 
                             keyspace_file=None):
        """
        """
        if self.get_instances() :
            raise Exception("This cluster is already running.  It must be terminated prior to being launched again.")

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
                                     keyspace_file=None, new_cluster=True, tokens=None):
        """
        """
        # we need all instances for seeds, but we should only transfer to new instances!
        all_instances = self.get_instances()
        if new_cluster :
            potential_seeds = all_instances
        else :
            potential_seeds = [instance for instance in all_instances if instance not in instances]


        self.logger.debug("Waiting for %d Cassandra instance(s) to install..." % len(instances))
        for instance in instances:
            self._wait_for_cassandra_install(instance, ssh_options)

        self.logger.debug("Copying configuration files to %d Cassandra instances..." % len(instances))

        seed_ips = [str(instance.private_dns_name) for instance in potential_seeds[:2]]
        if tokens == None :
            tokens = self._get_evenly_spaced_tokens_for_n_instances(len(instances))

        # for each instance, generate a config file from the original file and upload it to
        # the cluster node
        for i in range(len(instances)):
            local_file, remote_file = self._modify_config_file(instance=instances[i],
                                                               config_file=config_file,
                                                               seed_ips=seed_ips,
                                                               token=str(tokens[i]),
                                                               auto_bootstrap=not new_cluster)

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

    def _modify_config_file(self, instance, config_file, seed_ips, token, set_tokens=True, auto_bootstrap=False):
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
            if auto_bootstrap :
                yaml['auto_bootstrap'] = 'true'
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
        print "NOTE: May not be accurate if the cluster just started or expanded."
        return self._run_nodetool(ssh_options, "ring", instance)

    def _run_nodetool(self, ssh_options, ntcommand, instance=None, return_output=False):
        if instance is None:
          instance = self.get_instances()[0]

        self.logger.debug("running nodetool on instance %s", instance.id)
        command = "/usr/local/apache-cassandra/bin/nodetool -h localhost %s" % ntcommand
        ssh_command = self._get_standard_ssh_command(instance, ssh_options, command)
        if return_output :
            stdout = subprocess.PIPE
        else :
            stdout = None
        proc = subprocess.Popen(ssh_command, shell=True, stdout=stdout)
        if return_output:
            return (proc.wait(), proc.stdout)
        else:
            return proc.wait()

    def _discover_ring(self, ssh_options, instance=None):
        parsers = [
        ('Address         Status State   Load            Owns    Token                                       \n', parse_nodeline, 2),
         None
        ]
        self.logger.debug("discovering ring...")
        retcode, output = self._run_nodetool(ssh_options, "ring", instance, True)
        self.logger.debug("node tool returned %d" % retcode)
        lines = output.readlines()
        for parse_info in parsers :
            if parse_info is None:
                raise Exception("I don't recognize the nodetool output - no parser found.")
            if lines[0] == parse_info[0]:
                break
        nodelines = lines[parse_info[2]:]
        self.logger.debug("found %d nodes" % len(nodelines))
        output.close()
        return [parse_info[1](nodeline) for nodeline in nodelines]

    def calc_down_nodes(self, ssh_options, instance=None):
        nodes = self._discover_ring(ssh_options, instance)
        return [node['token'] for node in nodes if node['status'] == 'Down']

    def replace_down_nodes(self, instance_template, ssh_options, config_file):
        down_tokens = self.calc_down_nodes(ssh_options)
        instance_template.number = len(down_tokens)
        self.expand_cluster(instance_template, ssh_options, config_file, [x-1 for x in down_tokens])
        self.remove_down_nodes(ssh_options)

    def remove_down_nodes(self, ssh_options, instance=None):
        nodes = self._discover_ring(ssh_options, instance)
        for node in nodes:
            if node['status'] == 'Down' and node['state'] == 'Normal':
                print "Removing node %s." % node['token']
                self._run_nodetool(ssh_options, 'removetoken %s' % node['token'], instance)

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

    def _validate_ring(self, instance, ssh_options):
        """Run nodetool to verify that a ring is valid."""

        command = "/usr/local/apache-cassandra/bin/nodetool -h %s ring" % instance.private_dns_name
        ssh_command = self._get_standard_ssh_command(instance, ssh_options, command)
        retcode = 0
        try:
            # some nodes can be down, but nodetool will still exit cleanly,
            # so doing some extra validation to ensure that all nodes of 
            # the ring are "Up" and "Normal" and manually set a bad return 
            # code otherwise
            ring_output = subprocess.check_output(ssh_command, shell=True, stderr=subprocess.STDOUT)
            for node in ring_output.splitlines()[3:]:
                nodesplit = node.split()
                self.logger.debug("Node %s is %s and %s" % (nodesplit[0], nodesplit[1], nodesplit[2]))
                if nodesplit[1].lower() != "up" and nodesplit[2].lower() != "normal":
                    self.logger.debug("Node %s ring is not healthy" % nodesplit[0])
                    retcode = 200
        except subprocess.CalledProcessError, e:
            self.logger.debug(e)
            retcode = e.returncode

        return retcode

    def start_cassandra(self, ssh_options, config_file, create_keyspaces=False, instances=None, print_ring=True, retry=False):
        """Start Cassandra services on instances.
        To validate that Cassandra is running, this will check the output of
        nodetool ring, make sure that gossip and thrift are running, and check
        that nodetool info reports Normal mode.  If these tests do not pass
        within the timeout threshold, it will retry up to
        self.MAX_RESTART_ATTEMPTS times to restart.  If after meeting the max
        allowed, it will raise a TimeoutException.
        """

        if retry:
            self.logger.info("Attempting to start again (%s of %s)" % (self.current_attempt-1, self.MAX_RESTART_ATTEMPTS))
            print("Cassandra failed to start - attempting to start again (%s of %s)" % (self.current_attempt-1, self.MAX_RESTART_ATTEMPTS))

        if instances is None:
            instances = self.get_instances()

        self.logger.debug("Starting Cassandra service on %d instance(s)..." % len(instances))

        for instance in instances:
            # check to see if a pidfile exists, AND that a process is running
            # with that pid (protecting against stray pidfiles)
            command = """if [[ ! $(ps aux | grep $(cat /root/cassandra.pid 2>/dev/null) 2>/dev/null | grep -v grep) ]]
            then 
                if [ -f /root/cassandra.pid ]; then rm -f /root/cassandra.pid; fi
                nohup /usr/local/apache-cassandra/bin/cassandra -p /root/cassandra.pid &> /root/cassandra.out &
            fi
            """
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
                if self.current_attempt <= self.MAX_RESTART_ATTEMPTS:
                    self.logger.info("Cassandra startup has failed, trying again; retry number %s (%s remain)" %
                        (self.current_attempt, self.MAX_RESTART_ATTEMPTS-self.current_attempt))
                    self.current_attempt += 1
                    self.stop_cassandra(ssh_options, instances)
                    
                    # cassandra needs some time to fully shutdown
                    time.sleep(5)
                    self.start_cassandra(ssh_options, config_file, create_keyspaces, instances, print_ring, retry=True)
                    return
                else:
                    raise TimeoutException()
           
            # does the ring look ok?
            ring_retcode = self._validate_ring(temp_instances[-1], ssh_options)

            # is gossip running?
            command = "/usr/local/apache-cassandra/bin/nodetool -h %s info | grep Gossip | grep true" % temp_instances[-1].private_dns_name
            ssh_command = self._get_standard_ssh_command(temp_instances[-1], ssh_options, command)
            gossip_retcode = subprocess.call(ssh_command, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)

            # are the netstats looking ok?
            command = '/usr/local/apache-cassandra/bin/nodetool -h %s netstats | grep "Mode: Normal"' % temp_instances[-1].private_dns_name
            ssh_command = self._get_standard_ssh_command(temp_instances[-1], ssh_options, command)
            netstats_retcode = subprocess.call(ssh_command, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)

            # is thrift running?
            command = "/bin/netstat -an | grep 9160"
            ssh_command = self._get_standard_ssh_command(temp_instances[-1], ssh_options, command)
            thrift_retcode = subprocess.call(ssh_command, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)

            if ring_retcode == 0 and gossip_retcode == 0 and netstats_retcode == 0 and thrift_retcode == 0:
                temp_instances.pop()
            else:
                if ring_retcode != 0:
                    self.logger.warn("Return code for 'nodetool ring' on '%s': %d" % (temp_instances[-1].id, ring_retcode))
                if gossip_retcode != 0:
                    self.logger.warn("Return code for 'nodetool info | grep Gossip' on '%s': %d" % (temp_instances[-1].id, gossip_retcode))
                if netstats_retcode != 0:
                    self.logger.warn("Return code for 'nodetool netstats | grep Normal' on '%s': %d" % (temp_instances[-1].id, netstats_retcode))
                if thrift_retcode != 0:
                    self.logger.warn("Return code for 'netstat | grep 9160' (thrift) on '%s': %d" % (temp_instances[-1].id, thrift_retcode))

        if create_keyspaces:
            self._create_keyspaces_from_definitions_file(instances[0], config_file, ssh_options)
        else:
            self.logger.debug("create_keyspaces is False. Skipping keyspace generation.")
        
        # TODO: Do I need to wait for the keyspaces to propagate before printing the ring?
        # print ring after everything started
        if print_ring:
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
