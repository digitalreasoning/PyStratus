# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from boto.exception import EC2ResponseError
from cloud.cluster import Cluster
from cloud.cluster import Instance
from cloud.cluster import RoleSyntaxException
from cloud.cluster import TimeoutException
from cloud.cluster import InstanceTerminatedException
from cloud.storage import JsonVolumeManager
from cloud.storage import JsonVolumeSpecManager
from cloud.storage import MountableVolume
from cloud.storage import Storage
from cloud.exception import VolumesStillInUseException
from cloud.util import xstr
from cloud.util import get_ec2_connection
from prettytable import PrettyTable
import logging
import os
import re
import subprocess
import sys
import time

logger = logging.getLogger(__name__)

CLOUD_PROVIDER = ("ec2", ('cloud.providers.ec2', 'Ec2Cluster'))

def _run_command_on_instance(instance, ssh_options, command):
  print "Running ssh %s root@%s '%s'" % \
    (ssh_options, instance.public_dns_name, command)
  retcode = subprocess.call("ssh %s root@%s '%s'" %
                           (ssh_options, instance.public_dns_name, command),
                           shell=True)
  print "Command running on %s returned with value %s" % \
    (instance.public_dns_name, retcode)

def _wait_for_volume(ec2_connection, volume_id):
  """
  Waits until a volume becomes available.
  """
  while True:
    volumes = ec2_connection.get_all_volumes([volume_id,])
    if volumes[0].status == 'available':
      break
    sys.stdout.write(".")
    sys.stdout.flush()
    time.sleep(1)

class Ec2Cluster(Cluster):
  """
  A cluster of EC2 instances. A cluster has a unique name.

  Instances running in the cluster run in a security group with the cluster's
  name, and also a name indicating the instance's role, e.g. <cluster-name>-foo
  to show a "foo" instance.
  """

  @staticmethod
  def get_clusters_with_role(role, state="running", region="us-east-1"):
    all_instances = get_ec2_connection(region).get_all_instances()
    clusters = []
    for res in all_instances:
      instance = res.instances[0]
      for group in res.groups:
        if group.id.endswith("-" + role) and instance.state == state:
          clusters.append(re.sub("-%s$" % re.escape(role), "", group.id))
    return clusters

  def __init__(self, name, config_dir, region):
    super(Ec2Cluster, self).__init__(name, config_dir, region)

    self.ec2Connection = get_ec2_connection(region)

  def get_provider_code(self):
    return "ec2"

  def _get_cluster_group_name(self):
    return self.name

  def _check_role_name(self, role):
    if not re.match("^[a-zA-Z0-9_+]+$", role):
      raise RoleSyntaxException("Invalid role name '%s'" % role)

  def _group_name_for_role(self, role):
    """
    Return the security group name for an instance in a given role.
    """
    self._check_role_name(role)
    return "%s-%s" % (self.name, role)

  def _get_group_names(self, roles):
    group_names = [self._get_cluster_group_name()]
    for role in roles:
      group_names.append(self._group_name_for_role(role))
    return group_names

  def _get_all_group_names(self):
    security_groups = self.ec2Connection.get_all_security_groups()
    security_group_names = \
      [security_group.name for security_group in security_groups]
    return security_group_names

  def _get_all_group_names_for_cluster(self):
    all_group_names = self._get_all_group_names()
    r = []
    if self.name not in all_group_names:
      return r
    for group in all_group_names:
      if re.match("^%s(-[a-zA-Z0-9_+]+)?$" % self.name, group):
        r.append(group)
    return r

  def _create_custom_security_groups(self, security_groups=[]):
    """
    For each security group that doesn't exist we create it
    """

    all_groups = self._get_all_group_names()

    for group in security_groups:
        if group not in all_groups:
            self.ec2Connection.create_security_group(group,
                 "Custom group: %s" % group)

  def _create_groups(self, role):
    """
    Create the security groups for a given role, including a group for the
    cluster if it doesn't exist.
    """
    self._check_role_name(role)
    security_group_names = self._get_all_group_names()

    cluster_group_name = self._get_cluster_group_name()
    if not cluster_group_name in security_group_names:
      self.ec2Connection.create_security_group(cluster_group_name,
                                               "Cluster (%s)" % (self.name))
      self.ec2Connection.authorize_security_group(cluster_group_name,
                                                  cluster_group_name)
      # Allow SSH from anywhere
      self.ec2Connection.authorize_security_group(cluster_group_name,
                                                  ip_protocol="tcp",
                                                  from_port=22, to_port=22,
                                                  cidr_ip="0.0.0.0/0")

    role_group_name = self._group_name_for_role(role)
    if not role_group_name in security_group_names:
      self.ec2Connection.create_security_group(role_group_name,
        "Role %s (%s)" % (role, self.name))

  def authorize_role(self, role, from_port, to_port, cidr_ip):
    """
    Authorize access to machines in a given role from a given network.
    """
    self._check_role_name(role)
    role_group_name = self._group_name_for_role(role)
    # Revoke first to avoid InvalidPermission.Duplicate error
    self.ec2Connection.revoke_security_group(role_group_name,
                                             ip_protocol="tcp",
                                             from_port=from_port,
                                             to_port=to_port, cidr_ip=cidr_ip)
    self.ec2Connection.authorize_security_group(role_group_name,
                                                ip_protocol="tcp",
                                                from_port=from_port,
                                                to_port=to_port,
                                                cidr_ip=cidr_ip)

  def _get_instances(self, group_name, state_filter=None):
    """
    Get all the instances in a group, filtered by state.

    @param group_name: the name of the group
    @param state_filter: the state that the instance should be in
      (e.g. "running"), or None for all states
    """
    all_instances = self.ec2Connection.get_all_instances()
    instances = []
    for res in all_instances:
      for group in res.groups:
        if group.id == group_name:
          for instance in res.instances:
            if state_filter == None or instance.state == state_filter:
              instances.append(instance)
    return instances

  def get_instances_in_role(self, role, state_filter=None):
    """
    Get all the instances in a role, filtered by state.

    @param role: the name of the role
    @param state_filter: the state that the instance should be in
      (e.g. "running"), or None for all states
    """
    self._check_role_name(role)

    instances = self._get_instances(self._group_name_for_role(role),
                               state_filter)
    for i in instances:
        i.__setattr__('role', role)
    return instances
    """
    instances = []
    for instance in self._get_instances(self._group_name_for_role(role),
                                        state_filter):
      instances.append(Instance(instance.id, role, instance.dns_name,
                                instance.private_dns_name,
                                instance.launch_time,
                                instance.instance_type,
                                instance.placement))
    return instances
    """

  def _print_instance(self, role, instance):
    print "\t".join((role, instance.id,
      instance.image_id,
      instance.dns_name, instance.private_dns_name,
      instance.state, xstr(instance.key_name), instance.instance_type,
      str(instance.launch_time), instance.placement))

  def _get_instance_status_headers(self):
    return ("Role", "Instance Id", "Image Id", "Public DNS", "Private DNS",
            "State", "Key", "Instance Type", "Launch Time", "Zone")

  def _get_instance_status(self, role, instance):
    return (role, instance.id,
            instance.image_id,
            instance.dns_name, instance.private_dns_name,
            instance.state, xstr(instance.key_name), instance.instance_type,
            str(instance.launch_time), instance.placement)

  def get_instances(self, roles=None, state_filter="running"):
    """
    Returns a list of Instance objects in this cluster
    """
    if roles is None:
      return self._get_instances(self._get_cluster_group_name(), state_filter)
    else:
      instances = []
      for role in roles:
        instances.extend(self._get_instances(self._group_name_for_role(role),
                                        state_filter))
      return instances

  def print_status(self, roles=None, state_filter="running"):
    """
    Print the status of instances in the given roles, filtered by state.
    """
    table = PrettyTable()
    table.set_field_names(self._get_instance_status_headers())
    if not roles:
      for instance in self._get_instances(self._get_cluster_group_name(),
                                          state_filter):
        table.add_row(self._get_instance_status("", instance))
    else:
      for role in roles:
        for instance in self._get_instances(self._group_name_for_role(role),
                                            state_filter):
          table.add_row(self._get_instance_status(role, instance))

    if len(table.rows):
        table.printt()
        print "Total instances: %d" % len(table.rows)
    else:
        print "No running instances."

  def launch_instances(self, roles, number, image_id, size_id,
                       instance_user_data, **kwargs):
    for role in roles:
      self._check_role_name(role)
      self._create_groups(role)

    user_data = instance_user_data.read_as_gzip_stream()
    security_groups = self._get_group_names(roles) + kwargs.get('security_groups', [])

    # create groups from config that may not exist
    self._create_custom_security_groups(security_groups)

    reservation = self.ec2Connection.run_instances(image_id, min_count=number,
      max_count=number, key_name=kwargs.get('key_name', None),
      security_groups=security_groups, user_data=user_data,
      instance_type=size_id, placement=kwargs.get('placement', None))
    return [instance.id for instance in reservation.instances]

  def wait_for_instances(self, instance_ids, timeout=600, fail_on_terminated=True):
    wait_time = 3
    start_time = time.time()
    while True:
      if (time.time() - start_time >= timeout):
        raise TimeoutException()
      try:
        if self._all_started(self.ec2Connection.get_all_instances(instance_ids), fail_on_terminated):
          break
      # don't timeout for race condition where instance is not yet registered
      except EC2ResponseError, e:
        pass
      logging.info("Sleeping for %d seconds..." % wait_time)
      time.sleep(wait_time)

  def _all_started(self, reservations, fail_on_terminated=True):
    for res in reservations:
      for instance in res.instances:
        logging.info("Instance %s state = %s" % (instance, instance.state))

        # check for terminated
        if fail_on_terminated and instance.state == "terminated":
            raise InstanceTerminatedException(instance.state_reason['message'])

        if instance.state != "running":
          return False
    return True

  def terminate(self):
    instances = self._get_instances(self._get_cluster_group_name(), "running")
    if instances:
      self.ec2Connection.terminate_instances([i.id for i in instances])

  def delete(self):
    """
    Delete the security groups for each role in the cluster, and the group for
    the cluster.
    """
    group_names = self._get_all_group_names_for_cluster()
    for group in group_names:
      self.ec2Connection.delete_security_group(group)

  def get_storage(self):
    """
    Return the external storage for the cluster.
    """
    return Ec2Storage(self)


class Ec2Storage(Storage):
  """
  Storage volumes for an EC2 cluster. The storage is associated with a named
  cluster. Metadata for the storage volumes is kept in a JSON file on the client
  machine (in a file called "ec2-storage-<cluster-name>.json" in the
  configuration directory).
  """

  @staticmethod
  def create_formatted_snapshot(cluster, size, availability_zone, image_id,
                                key_name, ssh_options):
    """
    Creates a formatted snapshot of a given size. This saves having to format
    volumes when they are first attached.
    """
    conn = cluster.ec2Connection
    print "Starting instance"
    reservation = conn.run_instances(image_id, key_name=key_name,
                                     placement=availability_zone)
    instance = reservation.instances[0]
    print "Waiting for instance %s" % instance
    try:
      cluster.wait_for_instances([instance.id,])
      print "Started instance %s" % instance.id
    except TimeoutException:
      terminated = conn.terminate_instances([instance.id,])
      print "Timeout...shutting down %s" % terminated
      return
    print
    print "Waiting 60 seconds before attaching storage"
    time.sleep(60)
    # Re-populate instance object since it has more details filled in
    instance.update()

    print "Creating volume of size %s in %s" % (size, availability_zone)
    volume = conn.create_volume(size, availability_zone)
    print "Created volume %s" % volume
    print "Attaching volume to %s" % instance.id
    volume.attach(instance.id, '/dev/sdj')

    _run_command_on_instance(instance, ssh_options, """
      while true ; do
        echo 'Waiting for /dev/sdj...';
        if [ -e /dev/sdj ]; then break; fi;
        sleep 1;
      done;
      mkfs.ext3 -F -m 0.5 /dev/sdj
    """)

    print "Detaching volume"
    conn.detach_volume(volume.id, instance.id)
    print "Creating snapshot"
    description = "Formatted %dGB snapshot created by PyStratus" % size
    snapshot = volume.create_snapshot(description=description)
    print "Created snapshot %s" % snapshot.id
    _wait_for_volume(conn, volume.id)
    print
    print "Deleting volume"
    volume.delete()
    print "Deleted volume"
    print "Stopping instance"
    terminated = conn.terminate_instances([instance.id,])
    print "Stopped instance %s" % terminated

  def __init__(self, cluster):
    super(Ec2Storage, self).__init__(cluster)
    self.config_dir = cluster.config_dir

  def _get_storage_filename(self):
    # create the storage directory if it doesn't already exist
    p = os.path.join(self.config_dir, ".storage")
    if not os.path.isdir(p):
        os.makedirs(p)
    return os.path.join(p, "ec2-storage-%s.json" % (self.cluster.name))

  def create(self, role, number_of_instances, availability_zone, spec_filename):
    spec_file = open(spec_filename, 'r')
    volume_spec_manager = JsonVolumeSpecManager(spec_file)
    volume_manager = JsonVolumeManager(self._get_storage_filename())
    for dummy in range(number_of_instances):
      mountable_volumes = []
      volume_specs = volume_spec_manager.volume_specs_for_role(role)
      for spec in volume_specs:
        logger.info("Creating volume of size %s in %s from snapshot %s" % \
                    (spec.size, availability_zone, spec.snapshot_id))
        volume = self.cluster.ec2Connection.create_volume(spec.size,
                                                          availability_zone,
                                                          spec.snapshot_id)
        mountable_volumes.append(MountableVolume(volume.id, spec.mount_point,
                                                 spec.device))
      volume_manager.add_instance_storage_for_role(role, mountable_volumes)

  def _get_mountable_volumes(self, role):
    storage_filename = self._get_storage_filename()
    volume_manager = JsonVolumeManager(storage_filename)
    return volume_manager.get_instance_storage_for_role(role)

  def get_mappings_string_for_role(self, role):
    mappings = {}
    mountable_volumes_list = self._get_mountable_volumes(role)
    for mountable_volumes in mountable_volumes_list:
      for mountable_volume in mountable_volumes:
        mappings[mountable_volume.mount_point] = mountable_volume.device
    return ";".join(["%s,%s,%s" % (role, mount_point, device) for (mount_point, device)
                     in mappings.items()])

  def _has_storage(self, role):
    return self._get_mountable_volumes(role)

  def has_any_storage(self, roles):
    for role in roles:
      if self._has_storage(role):
        return True
    return False

  def get_roles(self):
    storage_filename = self._get_storage_filename()
    volume_manager = JsonVolumeManager(storage_filename)
    return volume_manager.get_roles()

  def _get_ec2_volumes_dict(self, mountable_volumes):
    volume_ids = [mv.volume_id for mv in sum(mountable_volumes, [])]
    volumes = self.cluster.ec2Connection.get_all_volumes(volume_ids)
    volumes_dict = {}
    for volume in volumes:
      volumes_dict[volume.id] = volume
    return volumes_dict

  def get_volumes(self, roles=None, volumes=None):
    result = []
    if volumes is not None:
      for r, v in volumes:
        result.append((r,v))
    else:
      if roles is None:
        storage_filename = self._get_storage_filename()
        volume_manager = JsonVolumeManager(storage_filename)
        roles = volume_manager.get_roles()
      for role in roles:
        mountable_volumes_list = self._get_mountable_volumes(role)
        ec2_volumes = self._get_ec2_volumes_dict(mountable_volumes_list)
        for mountable_volumes in mountable_volumes_list:
          for mountable_volume in mountable_volumes:
            result.append((role, ec2_volumes[mountable_volume.volume_id]))
    return result

  def _replace(self, string, replacements):
    for (match, replacement) in replacements.iteritems():
      string = string.replace(match, replacement)
    return string

  def check(self):
    storage_filename = self._get_storage_filename()
    volume_manager = JsonVolumeManager(storage_filename)

    all_mountable_volumes = []
    roles = volume_manager.get_roles()
    for r in roles:
        all_mountable_volumes.extend(sum(self._get_mountable_volumes(r),[]))

    if not all_mountable_volumes:
        print "No EBS volumes found. Have you executed 'create-storage' first?"
        return

    error = False

    # disable boto ERROR logging for now
    boto_logging = logging.getLogger('boto')
    level = boto_logging.level
    boto_logging.setLevel(logging.FATAL)

    for vid in [v.volume_id for v in all_mountable_volumes]:
        try:
            self.cluster.ec2Connection.get_all_volumes([vid])
        except:
            error = True
            print "Volume does not exist: %s" % vid

    if not error:
        print "Congrats! All volumes exist!"

    # reset boto logging
    boto_logging.setLevel(level)

  def attach(self, role, instances):
    mountable_volumes_list = self._get_mountable_volumes(role)
    if not mountable_volumes_list:
      return
    ec2_volumes = self._get_ec2_volumes_dict(mountable_volumes_list)

    available_mountable_volumes_list = []

    available_instances_dict = {}
    for instance in instances:
      available_instances_dict[instance.id] = instance

    # Iterate over mountable_volumes and retain those that are not attached
    # Also maintain a list of instances that have no attached storage
    # Note that we do not fill in "holes" (instances that only have some of
    # their storage attached)
    for mountable_volumes in mountable_volumes_list:
      available = True
      for mountable_volume in mountable_volumes:
        if ec2_volumes[mountable_volume.volume_id].status != 'available':
          available = False
          attach_data = ec2_volumes[mountable_volume.volume_id].attach_data
          instance_id = attach_data.instance_id
          if available_instances_dict.has_key(instance_id):
            del available_instances_dict[instance_id]
      if available:
        available_mountable_volumes_list.append(mountable_volumes)

    if len(available_instances_dict) != len(available_mountable_volumes_list):
      logger.warning("Number of available instances (%s) and volumes (%s) \
        do not match." \
        % (len(available_instances_dict),
           len(available_mountable_volumes_list)))

    for (instance, mountable_volumes) in zip(available_instances_dict.values(),
                                             available_mountable_volumes_list):
      print "Attaching storage to %s" % instance.id
      for mountable_volume in mountable_volumes:
        volume = ec2_volumes[mountable_volume.volume_id]
        print "Attaching %s to %s" % (volume.id, instance.id)
        volume.attach(instance.id, mountable_volume.device)

  def delete(self, roles=[]):
    storage_filename = self._get_storage_filename()
    volume_manager = JsonVolumeManager(storage_filename)
    for role in roles:
      mountable_volumes_list = volume_manager.get_instance_storage_for_role(role)
      ec2_volumes = self._get_ec2_volumes_dict(mountable_volumes_list)
      all_available = True
      for volume in ec2_volumes.itervalues():
        if volume.status != 'available':
          all_available = False
          logger.warning("Volume %s is not available.", volume)
      if not all_available:
        msg = "Some volumes are still in use. Aborting delete."
        logger.warning(msg)
        raise VolumesStillInUseException(msg)
      for volume in ec2_volumes.itervalues():
        volume.delete()
      volume_manager.remove_instance_storage_for_role(role)

  def create_snapshot_of_all_volumes(self, cluster_name, volumes=[]):
    for r, v in volumes:
      description=",".join((v.id, r, cluster_name))
      print "Creating snapshot with description %s" % description
      self.cluster.ec2Connection.create_snapshot(v.id, description=description)
