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

class CassandraServiceCLI(CLIPlugin):
    USAGE = """Cassandra service usage: CLUSTER COMMAND [OPTIONS]
where COMMAND and [OPTIONS] may be one of:
            
                               CASSANDRA COMMANDS
  ----------------------------------------------------------------------------------
  start-cassandra                     starts the cassandra service on all nodes
  stop-cassandra                      stops the cassandra service on all nodes
  print-ring [INSTANCE_IDX]           displays the cluster's ring information
  rebalance                           recalculates tokens evenly and moves nodes
  remove-down-nodes                   removes nodes that are down from the ring

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
    
    def __init__(self):
        super(CassandraServiceCLI, self).__init__()

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

        elif self._command_name == "replace-down-nodes":
            self.replace_down_nodes(argv, options_dict)

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

        elif self._command_name == "stop-cassandra":
            self.stop_cassandra(argv, options_dict)

        elif self._command_name == "start-cassandra":
            self.start_cassandra(argv, options_dict)

        elif self._command_name == "print-ring":
            self.print_ring(argv, options_dict)

        elif self._command_name == "rebalance":
            self.rebalance(argv, options_dict)

        elif self._command_name == "remove-down-nodes":
            self.remove_down_nodes(argv, options_dict)

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

        # test files
        for key in ['cassandra_config_file']:
            if opt.get(key) is not None:
                try:
                    url = urllib.urlopen(opt.get(key))
                    data = url.read()
                except:
                    raise
                    print "The file defined by %s (%s) does not exist. Aborting." % (key, opt.get(key))
                    sys.exit(1)

        number_of_nodes = int(args[0])
        instance_template = InstanceTemplate(
            (self.service.CASSANDRA_NODE,),
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
                                    opt.get('ssh_options'),
                                    opt.get('cassandra_config_file'))

    def replace_down_nodes(self, argv, options_dict):
        opt, args = self.parse_options(self._command_name,
                                       argv)
        opt.update(options_dict)

        # check for the cassandra-specific files
        if opt.get('cassandra_config_file') is None:
            print "ERROR: No cassandra_config_file configured. Aborting."
            sys.exit(1)

        # test files
        for key in ['cassandra_config_file']:
            if opt.get(key) is not None:
                try:
                    url = urllib.urlopen(opt.get(key))
                    data = url.read()
                except:
                    raise
                    print "The file defined by %s (%s) does not exist. Aborting." % (key, opt.get(key))
                    sys.exit(1)

        tokens = self.service.calc_down_nodes(opt.get('ssh_options'))
        number_of_nodes = len(tokens)
        instance_template = InstanceTemplate(
            (self.service.CASSANDRA_NODE,),
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
                                    opt.get('ssh_options'),
                                    opt.get('cassandra_config_file'),
                                    tokens)

    def launch_cluster(self, argv, options_dict):
        """
        """
        expected_arguments = ["NUM_INSTANCES"]
        opt, args = self.parse_options(self._command_name, 
                                      argv,
                                      expected_arguments=expected_arguments)
        opt.update(options_dict)

        # check for the cassandra-specific files
        if opt.get('cassandra_config_file') is None:
            print "ERROR: No cassandra_config_file configured. Aborting."
            sys.exit(1)

        if opt.get('keyspace_definitions_file') is None:
            print "WARNING: No keyspace_definitions_file configured. You can ignore this for Cassandra v0.6.x"

        # test files
        for key in ['cassandra_config_file', 'keyspace_definitions_file']:
            if opt.get(key) is not None:
                try:
                    url = urllib.urlopen(opt.get(key))
                    data = url.read()
                except: 
                    raise
                    print "The file defined by %s (%s) does not exist. Aborting." % (key, opt.get(key))
                    sys.exit(1)

#        if self.service.get_instances() :
#            print "This cluster is already running.  It must be terminated prior to being launched again."
#            sys.exit(1)

        number_of_nodes = int(args[0])
        instance_template = InstanceTemplate(
            (self.service.CASSANDRA_NODE,), 
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
                                    opt.get('ssh_options'),
                                    opt.get('cassandra_config_file'),
                                    opt.get('keyspace_definitions_file')) 

    def stop_cassandra(self, argv, options_dict):
        instances = self.service.get_instances()
        if not instances:
            print "No running instances. Aborting."
            sys.exit(1)

        print "Stopping Cassandra service on %d instance(s)...please wait." % len(instances)
        self.service.stop_cassandra(options_dict.get('ssh_options'), instances=instances)

    def start_cassandra(self, argv, options_dict):
        instances = self.service.get_instances()
        if not instances:
            print "No running instances. Aborting."
            sys.exit(1)

        print "Starting Cassandra service on %d instance(s)...please wait." % len(instances)
        self.service.start_cassandra(options_dict.get('ssh_options'), options_dict.get('cassandra_config_file'), instances=instances)

    def print_ring(self, argv, options_dict):
        instances = self.service.get_instances()
        if not instances:
            print "No running instances. Aborting."
            sys.exit(1)

        idx = 0
        if len(argv) > 0 :
            idx = int(argv[0])
        self.service.print_ring(options_dict.get('ssh_options'), instances[idx])

    def rebalance(self, argv, options_dict):
        instances = self.service.get_instances()
        if not instances:
            print "No running instances. Aborting."
            sys.exit(1)

        self.service.rebalance(options_dict.get('ssh_options'))

    def remove_down_nodes(self, argv, options_dict):
        instances = self.service.get_instances()
        if not instances:
            print "No running instances. Aborting."
            sys.exit(1)

        self.service.remove_down_nodes(options_dict.get('ssh_options'))

    def create_storage(self, argv, options_dict):
        opt, args = self.parse_options(self._command_name, argv, BASIC_OPTIONS,
                                       ["NUM_INSTANCES", "SPEC_FILE"])
        opt.update(options_dict)

        role = self.service.CASSANDRA_NODE
        number_of_instances = int(args[0])
        spec_file = args[1]

        # FIXME
        # check_options_set(opt, ['availability_zone'])

        self.service.create_storage(role, 
                                    number_of_instances,
                                    opt.get('availability_zone'),
                                    spec_file)
        self.print_storage()

