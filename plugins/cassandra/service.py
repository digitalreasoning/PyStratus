import os
import sys
import time
import subprocess
import urllib
import tempfile
import socket

from fabric.api import *
from fabric.contrib import files

from cloud.cluster import TimeoutException
from cloud.service import InstanceTemplate
from cloud.plugin import ServicePlugin 
from cloud.util import exec_command
from cloud.util import use_sudo
from cloud.util import xstr
from cloud.util import check_output
from cloud.util import FULL_HIDE
from cloud.decorators import timeout

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
    fields = ("ip", "datacenter", "rack", "status", "state", "load", "distribution", "token")
    values = nodeline.split()
    values = values[:5] + [" ".join(values[5:7])] + values[7:]
    return dict(zip(fields, values))

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

    def expand_cluster(self, instance_template, new_tokens=None):
        instances = self.get_instances()
        if instance_template.number > len(instances):
            raise Exception("The best we can do is double the cluster size at one time.  Please specify %d instances or less." % len(instances))
        if new_tokens is None:
            existing_tokens = [node['token'] for node in self._discover_ring()]
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

        # pull the remote cassandra.yaml file, modify it, and push it back out
        self._modify_

        first = True
        for instance in new_instances:
            if not first:
                self.logger.info("Waiting 2 minutes before starting the next instance...")
                time.sleep(2*60)
            else:
                first = False
            self.logger.info("Starting cassandra on instance %s." % instance.id)
            self.start_cassandra(instances=[instance], print_ring=False)

        self.print_ring(instances[0])


    def launch_cluster(self, instance_template, options):
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
        
        # attach storage
        self._attach_storage(instance_template.roles)

        new_cluster = (len(instances) == len(instance_ids))

        # configure the individual instances
        self._configure_cassandra(new_instances, new_cluster=new_cluster)

        # start up the service
        self.start_cassandra(instances=new_instances)
    
    def _configure_cassandra(self, instances, new_cluster=True, tokens=None):
        """
        """
        # we need all instances for seeds, but we should only transfer to new instances!
        all_instances = self.get_instances()
        if new_cluster :
            potential_seeds = all_instances
        else :
            potential_seeds = [instance for instance in all_instances if instance not in instances]

        self.logger.debug("Configuring %d Cassandra instances..." % len(instances))

        seed_ips = [str(instance.private_dns_name) for instance in potential_seeds[:2]]
        if tokens == None :
            tokens = self._get_evenly_spaced_tokens_for_n_instances(len(instances))

        # wait for all instances to be ready
        print "WAITING FOR ALL INSTANCES"
        while all_instances:
            i = all_instances[-1]
            home = get_cassandra_home(i)
            print home

            if home == "":
                continue
            all_instances.pop()

        # for each instance, generate a config file from the original file and upload it to
        # the cluster node
        for i, instance in enumerate(instances):
            self._configure_cassandra_instance(instance=instance, 
                                               seed_ips=seed_ips, 
                                               token=str(tokens[i]), 
                                               auto_bootstrap=not new_cluster)

        #self.logger.debug("Waiting for %d Cassandra instance(s) to install..." % len(instances))
        #for instance in instances:
        #    self._wait_for_cassandra_service(instance)

    @timeout(600)
    def _wait_for_cassandra_service(self, instance):
        """
        Waiting for the cassandra.pid file
        """
        wait_time = 3
        with settings(host_string=instance.public_dns_name, warn_only=True):
            with FULL_HIDE:
                try:
                    while not files.exists("/var/run/cassandra.pid", use_sudo=use_sudo()):
                        self.logger.debug("Sleeping for %d seconds..." % wait_time)
                        time.sleep(wait_time)
                # catch SystemExit because paramiko will call abort when it detects a failure
                # in establishing an SSH connection
                except SystemExit:
                    pass

    def _configure_cassandra_instance(self, instance, seed_ips, token, set_tokens=True, auto_bootstrap=False):
        self.logger.debug("Configuring %s..." % instance.id)
        yaml_file = os.path.join("/tmp", "cassandra.yaml")
        cassandra_home = self.get_cassandra_home(instance)

        self.logger.debug("Local cassandra.yaml file: %s" % yaml_file)
        with settings(host_string=instance.public_dns_name, warn_only=True): #, hide("everything"):

            cassandra_data = os.path.join("/mnt", "cassandra-data")
            cassandra_logs = os.path.join("/mnt", "cassandra-logs")

            # create directories and log files
            exec_command("mkdir -p %s" % cassandra_data)
            exec_command("mkdir -p %s" % cassandra_logs)

            # set permissions
            exec_command("chown -R cassandra:cassandra %s %s" % (cassandra_data, cassandra_logs))

            try:
                # get yaml file
                get(os.path.join(cassandra_home, "conf", "cassandra.yaml"), "/tmp")

                # modify it
                f = open(yaml_file)
                yaml = parse_yaml(f)
                f.close()

                yaml['seed_provider'][0]['parameters'][0]['seeds'] = ",".join(seed_ips)
                if set_tokens is True :
                    yaml['initial_token'] = token
                if auto_bootstrap :
                    yaml['auto_bootstrap'] = 'true'
                yaml['data_file_directories'] = [cassandra_data]
                yaml['commitlog_directory'] = cassandra_logs
                yaml['listen_address'] = str(instance.private_dns_name)
                yaml['rpc_address'] = str(instance.public_dns_name)

                f = open(yaml_file, "w")
                f.write(dump_yaml(yaml))
                f.close()

                # put modified yaml file
                put(yaml_file, os.path.join(cassandra_home, "conf", "cassandra.yaml"), use_sudo=use_sudo())
            except SystemExit, e:
                raise
                pass

        os.unlink(yaml_file)

    def hack_config_for_multi_region(self, ssh_options, seeds):
        instances = self.get_instances()
        downloaded_file = os.path.join("/tmp", "cassandra.yaml.downloaded")
        for instance in instances:
            with settings(host_string=instance.public_dns_name, warn_only=True):
                # download config file
                print "downloading config from %s" % instance.public_dns_name
                get("/etc/cassandra/cassandra.yaml", downloaded_file)

                print "modifying config from %s" % instance.public_dns_name
                yaml = parse_yaml(urllib.urlopen(downloaded_file))
                yaml['seed_provider'][0]['parameters'][0]['seeds'] = seeds
                yaml['listen_address'] = str(instance.public_dns_name)
                yaml['rpc_address'] = str(instance.public_dns_name)
                yaml['broadcast_address'] = socket.gethostbyname(str(instance.public_dns_name))
                yaml['endpoint_snitch'] = 'org.apache.cassandra.locator.Ec2MultiRegionSnitch'
                
                print "saving config from %s" % instance.public_dns_name
                fd, temp_file = tempfile.mkstemp(prefix='cassandra.yaml_', text=True)
                os.write(fd, dump_yaml(yaml))
                os.close(fd)

                #upload config file
                print "uploading new config to %s" % instance.public_dns_name
                put(temp_file, "/etc/cassandra/cassandra.yaml", use_sudo=use_sudo())

                os.unlink(temp_file)
                os.unlink(downloaded_file)

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

    def print_ring(self, instance=None):
        # check to see if cassandra is running
        if not self.is_running(instance):
            return "Cassandra does not appear to be running."

        print "\nRing configuration..."
        print "NOTE: May not be accurate if the cluster just started or expanded.\n"
        return self._run_nodetool("ring", instance)

    def _run_nodetool(self, ntcommand, instance=None):
        if instance is None:
            instance = self.get_instances()[0]

        self.logger.debug("running nodetool on instance %s", instance.id)
        with settings(host_string=instance.public_dns_name, warn_only=True), hide("everything"):
            output = exec_command("nodetool -h %s %s" % (instance.private_dns_name, ntcommand))

        return output

    def _discover_ring(self, instance=None):
        if instance is None:
            instance = self.get_instances()[0]

        with settings(host_string=instance.public_dns_name, warn_only=True), hide("everything"):
            status = exec_command("service cassandra status")

            if status.failed:
                raise RuntimeException("Cassandra does not appear to be running.")

            self.logger.debug("Discovering ring...")
            retcode, output = self._run_nodetool("ring", instance)
            self.logger.debug("node tool output:\n%s" % output)
            lines = output.split("\n")[2:]

            assert len(lines) > 0, "Ring output must have more than two lines."

            self.logger.debug("Found %d nodes" % len(lines))
        
            return [parse_nodeline(line) for line in lines]

    def calc_down_nodes(self, instance=None):
        nodes = self._discover_ring(instance)
        return [node['token'] for node in nodes if node['status'] == 'Down']

    def replace_down_nodes(self, instance_template, config_file):
        down_tokens = self.calc_down_nodes()
        instance_template.number = len(down_tokens)
        self.expand_cluster(instance_template, config_file, [x-1 for x in down_tokens])
        self.remove_down_nodes()

    def remove_down_nodes(self, instance=None):
        nodes = self._discover_ring(instance)
        for node in nodes:
            if node['status'] == 'Down' and node['state'] == 'Normal':
                print "Removing node %s." % node['token']
                self._run_nodetool('removetoken %s' % node['token'], instance)

    def rebalance(self, offset=0):
        instances = self.get_instances()
        tokens = self._get_evenly_spaced_tokens_for_n_instances(len(instances))
        
        for token in tokens:
            #print "%s  --->  %s" % (token, (int(token)+offset))
            assert (int(token)+offset) <= 2**127, "Failed token: %s" % str((int(token)+offset))

        self.logger.info("new token space: %s" % str(tokens))
        for i, instance in enumerate(instances):
            token = str(int(tokens[i]) + offset)
            self.logger.info("Moving instance %s to token %s" % (instance.id, token))
            retcode, output = self._run_nodetool("move %s" % token, instance=instance)
            if retcode != 0 :
                self.logger.warn("Move failed for instance %s with return code %d..." % (instance.id, retcode))
                self.logger.warn(output)
            else :
                self.logger.info("Move succeeded for instance %s..." % instance.id)

    def _validate_ring(self, instance):
        """
        Run nodetool to verify that a ring is valid.
        """

        ring_output = exec_command("nodetool --host %s ring" % instance.private_dns_name)

        if ring_output.failed:
            return ring_output.return_code

        # some nodes can be down, but nodetool will still exit cleanly,
        # so doing some extra validation to ensure that all nodes of 
        # the ring are "Up" and "Normal" and manually set a bad return 
        # code otherwise
        retcode = 0
        for node in ring_output.splitlines()[3:]:
            #host = node[:16].strip()
            #data_center = node[16:28].strip()
            #rack = node[28:40].strip()
            #status = node[40:47].strip()
            #state = node[47

            nodesplit = node.split()

            self.logger.debug("Node %s is %s and %s" % (nodesplit[0], nodesplit[3], nodesplit[4]))
            if nodesplit[3].lower() != "up" and nodesplit[4].lower() != "normal":
                self.logger.debug("Node %s ring is not healthy" % nodesplit[0])
                self.logger.debug("Ring status:")
                self.logger.debug(ring_output)
                retcode = 200

        return retcode

    @timeout(600)
    def start_cassandra(self, instances=None, print_ring=True, retry=False):
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

        for instance in instances:
            with settings(host_string=instance.public_dns_name, warn_only=True): #, hide("everything"):
                errors = -1
                self.logger.info("Starting Cassandra service on %s..." % instance.id)

                while True:
                    try:
                        # check to see if cassandra is running

                        if self.is_running(instance):
                            self.logger.info("Cassandra is running.")
                            break

                        # start it if this is the first time
                        if errors < 0:
                            self.logger.info("Cassandra is not running. Attempting to start now...")
                            print("Cassandra is not running. Attempting to start now...")
                            exec_command("service cassandra start", pty=False)
                        elif errors >= 5:
                            #tail = sudo("tail -n 50 /var/log/cassandra/output.log")
                            #self.logger.error(tail)
                            raise RuntimeError("Unable to start cassandra. Check the logs for more information.")
                        self.logger.info("Error detecting Cassandra status...will try again in 3 seconds.")
                        errors += 1
                        time.sleep(3)

                    except SystemExit, e:
                        self.logger.error(str(e))

        # test connection
        self.logger.debug("Testing connection to each Cassandra instance...")

        temp_instances = instances[:]
        while len(temp_instances) > 0:
            instance = temp_instances[-1]

            with settings(host_string=instance.public_dns_name, warn_only=True), hide("everything"):
                # does the ring look ok?
                ring_retcode = self._validate_ring(instance)

                # is gossip running?
                gossip_retcode = exec_command("nodetool -h %s info | grep Gossip | grep true" % instance.private_dns_name).return_code

                # are the netstats looking ok?
                netstats_retcode = exec_command("nodetool -h %s netstats | grep 'Mode: NORMAL'" % instance.private_dns_name).return_code

                # is thrift running?
                thrift_retcode = exec_command("/bin/netstat -an | grep 9160").return_code

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

                    time.sleep(3)

        # print ring after everything started
        if print_ring:
            print self.print_ring(instances[0])

        self.logger.debug("Startup complete.")

    def stop_cassandra(self, instances=None):
        if instances is None:
          instances = self.get_instances()

        for instance in instances:
            self.logger.info("Stopping Cassandra on %s" % instance.id)
            with settings(host_string=instance.public_dns_name, warn_only=True), hide("everything"):
                result = exec_command("service cassandra stop")
                self.logger.info(result)

        self.logger.debug("Shutdown complete.")

    def get_cassandra_pid(self, instance):
        with settings(host_string=instance.public_dns_name, warn_only=True):
            pid = exec_command("cat /var/run/cassandra.pid")
            if pid.failed:
                return None
            return pid

    def is_running(self, instance):
        with settings(host_string=instance.public_dns_name), hide("everything"):
            return "is running" in exec_command("service cassandra status")

        #pid = self.get_cassandra_pid(instance)
        #if pid is None:
        #    return False
        #
        #with settings(host_string=instance.public_dns_name, warn_only=True):
        #    return exec_command("ps auxw | grep -v grep | grep %s" % pid).succeeded
        

    def get_cassandra_home(self, instance):
        with settings(host_string=instance.public_dns_name, warn_only=True):
            return exec_command("echo $CASSANDRA_HOME")
        
