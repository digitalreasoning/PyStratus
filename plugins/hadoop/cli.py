import sys
import logging
import urllib

from cloud.plugin import CLIPlugin
from cloud.plugin import BASIC_OPTIONS
from cloud.service import InstanceTemplate
from optparse import make_option
from prettytable import PrettyTable

class HadoopServiceCLI(CLIPlugin):
    USAGE = """Hadoop service usage: CLUSTER COMMAND [OPTIONS]
where COMMAND and [OPTIONS] may be one of:
            
                               HADOOP COMMANDS
  ----------------------------------------------------------------------------------
  launch-master                       launch or find a master in CLUSTER
  launch-slaves NUM_SLAVES            launch NUM_SLAVES slaves in CLUSTER

                               CLUSTER COMMANDS
  ----------------------------------------------------------------------------------
  details                             list instances in CLUSTER
  launch-cluster NUM_SLAVES           launch a master and NUM_SLAVES slaves in 
                                        CLUSTER
  terminate-cluster                   terminate all instances in CLUSTER
  login                               log in to the master in CLUSTER over SSH
  proxy                               start a SOCKS proxy on localhost into the
                                        CLUSTER

                               STORAGE COMMANDS
  ----------------------------------------------------------------------------------
  list-storage                        list storage volumes for CLUSTER
  create-storage NUM_INSTANCES        create volumes for NUM_INSTANCES instances
    SPEC_FILE                           for CLUSTER, using SPEC_FILE
  delete-storage                      delete all storage volumes for CLUSTER
"""
    
    def __init__(self):
        super(HadoopServiceCLI, self).__init__()
 
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

        elif self._command_name == "proxy":
            self.proxy(argv, options_dict)

        elif self._command_name == "terminate-cluster":
            self.terminate_cluster(argv, options_dict)

        elif self._command_name == "launch-cluster":
            self.launch_cluster(argv, options_dict)

        elif self._command_name == "login":
            self.login(argv, options_dict)

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

        else:
            self.print_help()

    def launch_cluster(self, argv, options_dict):
        """
        """

        expected_arguments = ["NUM_SLAVES"]
        opt, args = self.parse_options(self._command_name,
                                       argv,
                                       expected_arguments=expected_arguments)
        opt.update(options_dict)

        number_of_slaves = int(args[0])
        instance_templates = [
            InstanceTemplate(
                (
                    self.service.NAMENODE, 
                    self.service.SECONDARY_NAMENODE, 
                    self.service.JOBTRACKER
                ),
                1,
                opt.get('image_id'),
                opt.get('instance_type'), 
                opt.get('key_name'),
                opt.get('public_key'), 
                opt.get('user_data_file'),
                opt.get('availability_zone'), 
                opt.get('user_packages'),
                opt.get('auto_shutdown'), 
                opt.get('env'),
                opt.get('security_groups')),
            InstanceTemplate(
                (
                    self.service.DATANODE, 
                    self.service.TASKTRACKER
                ), 
                number_of_slaves,
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
        ]

        for it in instance_templates:
            it.add_env_strings([
                "CLUSTER_SIZE=%d" % (number_of_slaves+1)
            ])

        print "Launching cluster with %d instance(s)...please wait." % (number_of_slaves+1)
        jobtracker = self.service.launch_cluster(instance_templates, 
                                                 opt.get('client_cidr'),
                                                 opt.get('config_dir'))

        if jobtracker is None:
            print "An error occurred started the Hadoop service. Check the logs for more information."
            sys.exit(1)

        print "Browse the cluster at http://%s/" % jobtracker.public_dns_name
        self.logger.debug("Startup complete.")

    def create_storage(self, argv, options_dict):
        raise RuntimeError("Not implemented.")

        opt, args = self.parse_options(self._command_name, argv, BASIC_OPTIONS,
                                       ["NUM_INSTANCES", "SPEC_FILE"])
        opt.update(options_dict)

        role = CASSANDRA_NODE
        number_of_instances = int(args[0])
        spec_file = args[1]

        # FIXME
        # check_options_set(opt, ['availability_zone'])

        self.service.create_storage(role, 
                                    number_of_instances,
                                    opt.get('availability_zone'),
                                    spec_file)
        self.print_storage()

    def proxy(self, argv, options_dict):
        instances = self.service.get_instances()
        if not instances:
            "No running instances. Aborting."
            sys.exit(1)

        result = self.service.proxy(ssh_options=options_dict.get('ssh_options'),   
                                    instances=instances)

        if result is None:
            print "Unable to create proxy. Check logs for more information."
            sys.exit(1)

        print "Proxy created..."
        print """export HADOOP_CLOUD_PROXY_PID=%s;
echo Proxy pid %s;""" % (result, result)
