import itertools
import os
import subprocess
import sys
import logging
import time

from optparse import OptionParser
from optparse import make_option
from yapsy.IPlugin import IPlugin
from prettytable import PrettyTable

from cloud.cluster import InstanceUserData
from cloud.util import xstr
from cloud.util import build_env_string
from cloud.exception import VolumesStillInUseException

from cloud import VERSION

CONFIG_DIR_OPTION = \
  make_option("--config-dir", metavar="CONFIG-DIR",
    help="The configuration directory.")

PROVIDER_OPTION = \
  make_option("--cloud-provider", metavar="PROVIDER",
    help="The cloud provider, e.g. 'ec2' for Amazon EC2.")

AVAILABILITY_ZONE_OPTION = \
  make_option("-z", "--availability-zone", metavar="ZONE",
    help="The availability zone to run the instances in.")

REGION_OPTION = \
  make_option("-r", "--region", metavar="REGION",
    help="The region run the instances in.")

FORCE_OPTION = \
  make_option("--force", metavar="FORCE", 
              action="store_true", default=False,
              help="Force the command without prompting.")

BASIC_OPTIONS = [
  CONFIG_DIR_OPTION,
  PROVIDER_OPTION,
  AVAILABILITY_ZONE_OPTION,
  REGION_OPTION,
]

class CLIPlugin(IPlugin):
    """
    """
    USAGE = None

    def __init__(self, service=None):
        self.service = service
        self.logger = logging #logging.getLogger(self.__class__.__name__)

    def print_help(self, exitCode=1):
        if self.USAGE is None:
            raise RuntimeError("USAGE has not been defined.")

        print self.USAGE
        sys.exit(exitCode)

    def parse_options(self, command, argv, option_list=[], expected_arguments=[],
                      unbounded_args=False):
        """
        Parse the arguments to command using the given option list.

        If unbounded_args is true then there must be at least as many extra arguments
        as specified by extra_arguments (the first argument is always CLUSTER).
        Otherwise there must be exactly the same number of arguments as
        extra_arguments.
        """

        usage = "%%prog CLUSTER [options] %s" % \
            (" ".join([command] + expected_arguments[:]),)

        parser = OptionParser(usage=usage, version="%%prog %s" % VERSION,
                            option_list=option_list)

        parser.disable_interspersed_args()
        (options, args) = parser.parse_args(argv)
        if unbounded_args:
            if len(args) < len(expected_arguments):
                parser.error("incorrect number of arguments")
        elif len(args) != len(expected_arguments):
            parser.error("incorrect number of arguments")

        return (vars(options), args)

    def _prompt(self, prompt):
        """
        Returns true if user responds "yes" to prompt.
        """
        return raw_input("%s [yes or no]: " % prompt).lower() == "yes"
    
    def execute_command(self, argv, options_dict):
        """
        Should be overridden by the subclass to handle
        command specific options.
        """
        raise RuntimeError("Not implemented.")

    def create_storage(self, argv, options_dict):
        raise RuntimeError("Not implemented.")

    def terminate_cluster(self, argv, options_dict):
        opt, args = self.parse_options(self._command_name, argv, [FORCE_OPTION])

        if not self.service.get_instances():
            print "No running instances. Aborting."
            return

        if opt.get("force"):
            print "Terminating cluster..."
            self.service.terminate_cluster()
        else:
            self.print_instances()
            if not self._prompt("Terminate all instances?"):
                print "Not terminating cluster."
            else:
                print "Terminating cluster..."
                self.service.terminate_cluster()

    def simple_print_instances(self, argv, options_dict):
        opt, fields = self.parse_options(self._command_name, argv, expected_arguments=['FIELD*'], unbounded_args=True)

        for instance in self.service.get_instances():
            print("|".join([instance.__getattribute__(field) for field in fields]))

    def print_instances(self):
        if not self.service.get_instances():
            print "No running instances. Aborting."
            return

        table = PrettyTable()
        table.set_field_names(("Role", "Instance Id", "Image Id", 
                               "Public DNS", "Private DNS", "State", 
                               "Key", "Instance Type", "Launch Time", 
                               "Zone", "Region"))
        
        for i in self.service.get_instances():
            table.add_row((
                i.role, i.id, i.image_id, i.public_dns_name, 
                i.private_dns_name, i.state, i.key_name, i.instance_type,
                i.launch_time, i.placement, i.region.name))

        table.printt()

    def print_storage(self):
        storage = self.service.get_storage()
        
        table = PrettyTable()
        table.set_field_names(("Role", "Instance ID", "Volume Id", 
                               "Volume Size", "Snapshot Id", "Zone", 
                               "Status", "Device", "Create Time", 
                               "Attach Time"))

        for (r, v) in storage.get_volumes():
            table.add_row((r, v.attach_data.instance_id, v.id, 
                           str(v.size), v.snapshot_id, v.zone,
                           "%s / %s" % (v.status, v.attach_data.status), 
                           v.attach_data.device, str(v.create_time),
                           str(v.attach_data.attach_time)))
            
        if len(table.rows) > 0:
            s = 0
            for r in table.rows:
                s += int(r[3])

            table.printt()
            print "Total volumes: %d" % len(table.rows)
            print "Total size:    %d" % s
        else:
            print "No volumes defined."
    
    def delete_storage(self, argv, options_dict):
        opt, args = self.parse_options(self._command_name, argv, [FORCE_OPTION])

        storage = self.service.get_storage()
        volumes = storage.get_volumes()

        if not volumes:
            print "No volumes defined."
            sys.exit()

        if opt.get('force'):
            print "Deleting storage..."
            try:
                storage.delete(storage.get_roles())
            except VolumesStillInUseException, e:
                print e.message
                sys.exit(1)
        else:
            self.print_storage()
            if not self._prompt("Delete all storage volumes? THIS WILL PERMANENTLY DELETE ALL DATA"):
                print "Not deleting storage."
            else:
                print "Deleting storage..."
                try:
                    storage.delete(storage.get_roles())
                except VolumesStillInUseException, e:
                    print e.message
                    sys.exit(1)

    def login(self, argv, options_dict):
        """
        """
        instances = self.service.get_instances()
        if not instances:
            print "No running instances. Aborting."
            return

        table = PrettyTable()
        table.set_field_names(("", "ROLE", "INSTANCE ID", "PUBLIC IP", "PRIVATE IP"))

        for instance in instances:
            table.add_row((len(table.rows)+1, 
                           instance.role,
                           instance.id, 
                           instance.public_dns_name, 
                           instance.private_dns_name))

        table.printt()

        while True:
            try:
                choice = raw_input("Instance to login to [Enter = quit]: ")
                if choice == "":
                    sys.exit(0)
                choice = int(choice)
                if choice > 0 and choice <= len(table.rows):
                    instance = instances[choice-1]
                    self.service.login(instance, options_dict.get('ssh_options'))
                    break
                else:
                    print "Not a valid choice. Try again."
            except ValueError:
                print "Not a valid choice. Try again."

    def transfer_files(self, argv, options_dict):
        opt, args = self.parse_options(self._command_name, argv, expected_arguments=['FILE_NAME*'], unbounded_args=True)
        result = self.service.transfer_files(args, options_dict.get('ssh_options'))

        table = PrettyTable()
        table.set_field_names(("INSTANCE ID", "PUBLIC IP", "PRIVATE IP", "FILE NAME", "RESULT"))
        for instance, file, retcode in result:
            table.add_row((instance.id,
                           instance.public_dns_name,
                           instance.private_dns_name,
                           file,
                           retcode
                           ))
        table.printt()

    def run_command(self, argv, options_dict):
        opt, args = self.parse_options(self._command_name, argv, expected_arguments=['COMMAND'])
        result = self.service.run_command(args[0], options_dict.get('ssh_options'))

        table = PrettyTable()
        table.set_field_names(("INSTANCE ID", "PUBLIC IP", "PRIVATE IP", "RESULT"))
        for instance, retcode in result:
            table.add_row((instance.id,
                           instance.public_dns_name,
                           instance.private_dns_name,
                           retcode
                           ))
        table.printt()



class ServicePlugin(object):
    def __init__(self, cluster=None):
        self.cluster = cluster
        self.logger = logging #logging.getLogger(self.__class__.__name__)

    def get_roles(self):
        """
        Returns a list of role identifiers for this service type.
        """
        raise RuntimeError("Not implemented.")

    def get_instances(self):
        """
        Returns a list of running Instance objects from the cluster

        self.cluster.get_instances_in_role(ROLE, "running")
        """
        raise RuntimeError("Not implemented.")

    def launch_cluster(self):
        raise RuntimeError("Not implemented.")
        
    def terminate_cluster(self):
        """
        Terminates all instances in the cluster
        """
        # TODO: Clear all tags
        self.logger.info("Terminating cluster")
        self.cluster.terminate()

    def get_storage(self):
        return self.cluster.get_storage()
    
    def print_storage_status(self):
        storage = self.get_storage()
        if not os.path.isfile(storage._get_storage_filename()):
            storage.print_status(volumes=self._get_cluster_volumes(storage))
        else:
            storage.print_status()

    def _get_standard_ssh_command(self, instance, ssh_options, remote_command=None, username="root"):
        """
        Returns the complete SSH command ready for execution on the instance.
        """
        cmd = "ssh %s %s@%s" % (xstr(ssh_options), username,
                                       instance.public_dns_name)
        
        if remote_command is not None:
            cmd += " '%s'" % remote_command

        return cmd

    def _attach_storage(self, roles):
        storage = self.cluster.get_storage()
        if storage.has_any_storage(roles):
            print "Waiting 10 seconds before attaching storage"
            time.sleep(10)
            for role in roles:
                storage.attach(role, self.cluster.get_instances_in_role(role, 'running'))
            storage.print_status(roles)

    def _launch_instances(self, instance_template, exclude_roles=[]):
        it = instance_template
        user_data_file_template = it.user_data_file_template
        
        if it.user_data_file_template == None:
            user_data_file_template = self._get_default_user_data_file_template()

        ebs_mappings = []
        storage = self.cluster.get_storage()
        for role in it.roles:
            if role in exclude_roles:
                continue 
            if storage.has_any_storage((role,)):
                ebs_mappings.append(storage.get_mappings_string_for_role(role))

        replacements = {
            "%ENV%": build_env_string(it.env_strings, {
                "ROLES": ",".join(it.roles),
                "USER_PACKAGES": it.user_packages,
                "AUTO_SHUTDOWN": it.auto_shutdown,
                "EBS_MAPPINGS": ";".join(ebs_mappings),
            })
        }
        self.logger.debug("EBS Mappings: %s" % ";".join(ebs_mappings))
        instance_user_data = InstanceUserData(user_data_file_template, replacements)

        self.logger.debug("InstanceUserData gzipped length: %d" % len(instance_user_data.read_as_gzip_stream()))

        instance_ids = self.cluster.launch_instances(it.roles, 
                                                     it.number, 
                                                     it.image_id,
                                                     it.size_id,
                                                     instance_user_data,
                                                     key_name=it.key_name,
                                                     public_key=it.public_key,
                                                     placement=it.placement,
                                                     security_groups=it.security_groups)

        self.logger.debug("Instance ids reported to start: %s" % str(instance_ids))
        return instance_ids

    def delete_storage(self, force=False):
        storage = self.cluster.get_storage()
        self._print_storage_status(storage)
        if not force and not self._prompt("Delete all storage volumes? THIS WILL \
    PERMANENTLY DELETE ALL DATA"):
            print "Not deleting storage volumes."
        else:
            print "Deleting storage"
            storage.delete(storage.get_roles())
    
    def create_storage(self, role, number_of_instances, availability_zone, spec_file):
        storage = self.get_storage()
        storage.create(role, number_of_instances, availability_zone, spec_file)

    def run_command(self, command, ssh_options):
        instances = self.get_instances()
        ssh_commands = [self._get_standard_ssh_command(instance, ssh_options=ssh_options, remote_command=command)
                    for instance in instances]
        procs = [subprocess.Popen(ssh_command, shell=True) for ssh_command in ssh_commands]
        retcodes = [proc.wait() for proc in procs]
        return zip(instances, retcodes)

    def _get_transfer_command(self, instance, file_name, ssh_options):
        transfer_command = "scp %s %s root@%s:" % (xstr(ssh_options), file_name, instance.public_dns_name)
#        transfer_command = self._get_standard_ssh_command(instance, ssh_options, "cat > %s" % file_name) + " < %s" % file_name
        self.logger.debug("Transfer command: %s" % transfer_command)
        return transfer_command

    def transfer_files(self, file_names, ssh_options):
        instances = self.get_instances()
        operations = list(itertools.product(instances, file_names))
        ssh_commands = [self._get_transfer_command(instance, file_name, ssh_options) for instance, file_name in
                        operations]
        procs = [subprocess.Popen(ssh_command, shell=True) for ssh_command in ssh_commands]
        retcodes = [proc.wait() for proc in procs]
        return [(operation[0], operation[1], retcode) for operation, retcode in zip(operations, retcodes)]
