import os
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
            
                               APPLICATION COMMANDS
  ----------------------------------------------------------------------------------
  launch-load-balancer                launch a load balancer for CLUSTER
  launch-nodes NUM_NODES              launch NUM_NODES nodes in CLUSTER
  start-nodes                         start the nodes
  stop-nodes                          stop the nodes
  start-load-balancer                 start the load balancer
  stop-load-balancer                  stop the load balancer

                               CLUSTER COMMANDS
  ----------------------------------------------------------------------------------
  details                             list instances in CLUSTER
  launch-cluster NUM_NODES            launch NUM_NODES Cassandra nodes
  expand-cluster NUM_NODES            adds new nodes
  terminate-cluster                   terminate all instances in CLUSTER
  login                               log in to the master in CLUSTER over SSH

                               STORAGE COMMANDS
  ----------------------------------------------------------------------------------
  list-storage                        list storage volumes for CLUSTER
  create-storage NUM_INSTANCES        create volumes for NUM_INSTANCES instances
    SPEC_FILE                           for CLUSTER, using SPEC_FILE
  delete-storage                      delete all storage volumes for CLUSTER
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

        # get spot configuration
        self._spot_config = {
                "spot_cluster": True if os.environ.get("SPOT_CLUSTER", options_dict.get("spot_cluster", "false")).lower() == "true" else False,
                "max_price": options_dict.get("max_price", None),
                "launch_group": options_dict.get("launch_group", None),
         }

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

        elif self._command_name == "create-storage":
            self.create_storage(argv, options_dict)

        elif self._command_name == "delete-storage":
            self.delete_storage(argv, options_dict)

        elif self._command_name == "list-storage":
            self.print_storage()

        else:
            self.print_help()

    def expand_cluster(self, argv, options_dict):
        expected_arguments = ["NUM_INSTANCES"]
        opt, args = self.parse_options(self._command_name,
                                       argv,
                                       expected_arguments=expected_arguments,
                                       unbounded_args=True)
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
            opt.get('security_groups'),
            self._spot_config
        )

#        instance_template.add_env_strings(["CLUSTER_SIZE=%d" % number_of_nodes])

        print "Expanding cluster by %d instance(s)...please wait." % number_of_nodes

        self.service.expand_cluster(instance_template,
                                    opt.get('ssh_options'),opt.get('wait_dir', '/'))

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
            opt.get('security_groups'),
            self._spot_config
        )

        instance_template.add_env_strings(["CLUSTER_SIZE=%d" % number_of_nodes])

        print "Launching cluster with %d instance(s)...please wait." % number_of_nodes

        self.service.launch_cluster(instance_template,
                                    opt.get('ssh_options'),opt.get('wait_dir', '/'))

    def create_storage(self, argv, options_dict):
        opt, args = self.parse_options(self._command_name, argv, BASIC_OPTIONS,
                                       ["NUM_INSTANCES", "SPEC_FILE"])
        opt.update(options_dict)

        role = self.service.SIMPLE_NODE
        number_of_instances = int(args[0])
        spec_file = args[1]

        # FIXME
        # check_options_set(opt, ['availability_zone'])

        self.service.create_storage(role, 
                                    number_of_instances,
                                    opt.get('availability_zone'),
                                    spec_file)
        self.print_storage()
