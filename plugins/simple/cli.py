import sys
import logging
import urllib

from cloud.plugin import CLIPlugin
from cloud.plugin import BASIC_OPTIONS
from cloud.service import InstanceTemplate
from optparse import make_option
from prettytable import PrettyTable
from pprint import pprint

# Add options here to override what's in the clusters.cfg file
# TODO

class SimpleServiceCLI(CLIPlugin):
    USAGE = """Simple service usage: CLUSTER COMMAND [OPTIONS]
where COMMAND and [OPTIONS] may be one of:
            
                               CLUSTER COMMANDS
  ----------------------------------------------------------------------------------
  details                             list instances in CLUSTER
  launch-cluster NUM_NODES            launch NUM_NODES Cassandra nodes
  expand-cluster NUM_NODES            adds new nodes
  terminate-cluster                   terminate all instances in CLUSTER
  login                               log in to the master in CLUSTER over SSH
"""
#  transfer FILE DESTINATION           transfer a file to all nodes
#  execute COMMAND                     execute a command on all nodes

    def __init__(self):
        super(SimpleServiceCLI, self).__init__()

        #self._logger = logging.getLogger("CassandraServiceCLI")
 
    def execute_command(self, argv, options_dict):
        if len(argv) < 2:
            self.print_help()

        self._cluster_name = argv[0]
        self._command_name = argv[1]

        # strip off the cluster name and command from argv
        argv = argv[2:]

        # handle all known commands and error on an unknown command
        if self._command_name == "details":
            self.print_instances()

        elif self._command_name == "simple-details":
            self.simple_print_instances(argv, options_dict)

        elif self._command_name == "terminate-cluster":
            self.terminate_cluster(argv, options_dict)

        elif self._command_name == "launch-cluster":
            self.launch_cluster(argv, options_dict)

        elif self._command_name == "expand-cluster":
            self.expand_cluster(argv, options_dict)

        elif self._command_name == "login":
            self.login(argv, options_dict)

        elif self._command_name == "run-command":
            self.run_command(argv, options_dict)

        elif self._command_name == "transfer-files":
            self.transfer_files(argv, options_dict)

        else:
            self.print_help()

    def expand_cluster(self, argv, options_dict):
        expected_arguments = ["NUM_INSTANCES"]
        opt, args = self.parse_options(self._command_name,
                                       argv,
                                       expected_arguments=expected_arguments,
                                       unbounded_args=True)
        opt.update(options_dict)

        # check for the cassandra-specific files
        if opt.get('cassandra_config_file') is None:
            print "ERROR: No cassandra_config_file configured. Aborting."
            sys.exit(1)

        number_of_nodes = int(args[0])
        instance_template = InstanceTemplate(
            (self.service.SIMPLE_NODE,),
            number_of_nodes,
            opt.get('image_id'),
            opt.get('instance_type'),
            opt.get('key_name'),
            opt.get('public_key'),
            opt.get('user_data_file'),
            opt.get('availability_zone'),
            opt.get('user_packages'),
            opt.get('auto_shutdown'),
            opt.get('env'),
            opt.get('security_groups'))
#        instance_template.add_env_strings(["CLUSTER_SIZE=%d" % number_of_nodes])

        print "Expanding cluster by %d instance(s)...please wait." % number_of_nodes

        self.service.expand_cluster(instance_template,
                                    opt.get('ssh_options'))

    def launch_cluster(self, argv, options_dict):
        """
        """
        expected_arguments = ["NUM_INSTANCES"]
        opt, args = self.parse_options(self._command_name, 
                                      argv,
                                      expected_arguments=expected_arguments)
        opt.update(options_dict)

        number_of_nodes = int(args[0])
        instance_template = InstanceTemplate(
            (self.service.SIMPLE_NODE,),
            number_of_nodes,
            opt.get('image_id'),
            opt.get('instance_type'),
            opt.get('key_name'),
            opt.get('public_key'), 
            opt.get('user_data_file'),
            opt.get('availability_zone'), 
            opt.get('user_packages'),
            opt.get('auto_shutdown'), 
            opt.get('env'),
            opt.get('security_groups'))
        instance_template.add_env_strings(["CLUSTER_SIZE=%d" % number_of_nodes])

        print "Launching cluster with %d instance(s)...please wait." % number_of_nodes

        self.service.launch_cluster(instance_template,
                                    opt.get('ssh_options'))
