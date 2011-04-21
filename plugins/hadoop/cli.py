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
  terminate-dead-nodes                find and terminate dead nodes in CLUSTER

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
  create-storage ROLE NUM_INSTANCES   create volumes for NUM_INSTANCES instances of
    SPEC_FILE                           type ROLE for CLUSTER, using SPEC_FILE
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

        elif self._command_name == "simple-details":
            self.simple_print_instances(argv, options_dict)

        elif self._command_name == "proxy":
            self.proxy(argv, options_dict)

        elif self._command_name == "terminate-cluster":
            self.terminate_cluster(argv, options_dict)

        elif self._command_name == "launch-cluster":
            self.launch_cluster(argv, options_dict)

        elif self._command_name == "terminate-dead-nodes":
            self.terminate_dead_nodes(argv, options_dict)

        elif self._command_name == "launch-master":
            self.launch_master(argv, options_dict)

        elif self._command_name == "launch-slaves":
            self.launch_slaves(argv, options_dict)

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

    def launch_master(self, argv, options_dict):
        """Launch the master node of a CLUSTER."""

        opt, args = self.parse_options(self._command_name, argv, BASIC_OPTIONS)
        opt.update(options_dict)
        
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
            ]

        print "Launching cluster master...please wait." 
        jobtracker = self.service.launch_cluster(instance_templates, 
                                                 opt.get('client_cidr'),
                                                 opt.get('config_dir'))

        if jobtracker is None:
            print "An error occurred started the Hadoop service. Check the logs for more information."
            sys.exit(1)

        print "Browse the cluster at http://%s/" % jobtracker.public_dns_name
        self.logger.debug("Startup complete.")

    def launch_slaves(self, argv, options_dict):
        """Launch slave/datanodes in CLUSTER."""

        expected_arguments = ["NUM_SLAVES"]
        opt, args = self.parse_options(self._command_name,
                                       argv,
                                       expected_arguments=expected_arguments)
        opt.update(options_dict)

        try:
            number_of_slaves = int(args[0])
        except ValueError:
            print("Number of slaves must be an integer")
            return

        instance_templates = [
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
                opt.get('security_groups')),
            ]

        # @todo - this is originally passed in when creating a cluster from
        # scratch, need to figure out what to do if we're growing a cluster
        #instance_template.add_env_strings([
        #    "CLUSTER_SIZE=%d" % (number_of_slaves+1)
        #])

        print("Launching %s slave%s for %s" % (number_of_slaves, 
            "" if number_of_slaves==1 else "s", self._cluster_name))

        # this is needed to filter the jobtracker/namenode down into
        # hadoop-site.xml for the new nodes
        namenode = self.service.get_namenode()
        jobtracker = self.service.get_jobtracker()
        for instance_template in instance_templates:
            instance_template.add_env_strings([
                "NN_HOST=%s" % namenode.public_dns_name,
                "JT_HOST=%s" % jobtracker.public_dns_name,
            ])

        # I think this count can be wrong if run too soon after running
        # terminate_dead_nodes
        existing_tasktrackers = self.service.get_tasktrackers()
        num_tasktrackers = len(existing_tasktrackers) if existing_tasktrackers else 0
        self.service.launch_cluster(instance_templates, 
            opt.get('client_cidr'), opt.get('config_dir'),
            num_existing_tasktrackers=num_tasktrackers)

    def terminate_dead_nodes(self, argv, options_dict):
        """Find and terminate dead nodes in CLUSTER."""

        opt, args = self.parse_options(self._command_name, argv, BASIC_OPTIONS)
        opt.update(options_dict)

        print("Looking for dead nodes in %s" % self._cluster_name)
        dead_nodes = self.service.find_dead_nodes(self._cluster_name, opt)
        if not dead_nodes:
            print("No dead nodes found")
            return 

        print ("Found %s dead nodes" % len(dead_nodes))
        self.service.terminate_nodes(dead_nodes, opt)

    def create_storage(self, argv, options_dict):
        opt, args = self.parse_options(self._command_name, argv, BASIC_OPTIONS,
                                       ["ROLE", "NUM_INSTANCES", "SPEC_FILE"])

        opt.update(options_dict)

        role = args[0]
        number_of_instances = int(args[1])
        spec_file = args[2]

        valid_roles = (self.service.NAMENODE, self.service.DATANODE)
        if role not in valid_roles:
            raise RuntimeError("Role must be one of '%s' or '%s'" % valid_roles)

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
