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

"""
Classes for running services on a cluster.
"""

from __future__ import with_statement

from cloud.settings import SERVICE_PROVIDER_MAP
from cloud.cluster import get_cluster
from cloud.cluster import InstanceUserData
from cloud.cluster import TimeoutException
from cloud.providers.ec2 import Ec2Storage
from cloud.util import build_env_string
from cloud.util import url_get
from cloud.util import xstr
from prettytable import PrettyTable
from datetime import datetime
import logging
import types
import os
import re
import socket
import subprocess
import sys
import time
import tempfile
import simplejson

logger = logging.getLogger(__name__) 

class InstanceTemplate(object):
  """
  A template for creating server instances in a cluster.
  """
  def __init__(self, roles, number, image_id, size_id,
                     key_name, public_key,
                     user_data_file_template=None, placement=None,
                     user_packages=None, auto_shutdown=None, env_strings=[],
                     security_groups=[]):
    self.roles = roles
    self.number = number
    self.image_id = image_id
    self.size_id = size_id
    self.key_name = key_name
    self.public_key = public_key
    self.user_data_file_template = user_data_file_template
    self.placement = placement
    self.user_packages = user_packages
    self.auto_shutdown = auto_shutdown
    self.env_strings = env_strings
    self.security_groups = security_groups

    t = type(self.security_groups)
    if t is types.NoneType:
        self.security_groups = []
    elif t is types.StringType:
        self.security_groups = [security_groups]

  def add_env_strings(self, env_strings):
    new_env_strings = list(self.env_strings or [])
    new_env_strings.extend(env_strings)
    self.env_strings = new_env_strings

class Service(object):
  """
  A general service that runs on a cluster.
  """
  
  def __init__(self, cluster):
    self.cluster = cluster
    
  def get_service_code(self):
    """
    The code that uniquely identifies the service.
    """
    raise Exception("Unimplemented")

  def get_clusters_for_provider(self, provider):
    """
    Find and return a list of clusters running in this type of provider. 
    """
    raise Exception("Unimplemented");
    
  def list_all(self, provider):
    """
    Find and print all clusters running this type of service.
    """
    raise Exception("Unimplemented")

  def list(self):
    """
    Find and print all the instances running in this cluster.
    """
    raise Exception("Unimplemented")
  
  def launch_master(self, instance_template, config_dir, client_cidr):
    """
    Launch a "master" instance.
    """
    raise Exception("Unimplemented")
  
  def launch_slaves(self, instance_template):
    """
    Launch "slave" instance.
    """
    raise Exception("Unimplemented")
  
  def launch_cluster(self, instance_templates, config_dir, client_cidr):
    """
    Launch a cluster of instances.
    """
    raise Exception("Unimplemented")
  
  def terminate_cluster(self, service, force=False):
    if not service.get_running_instances():
        print "No running instances. Aborting."
        sys.exit(0)

    service.list()
    if not force and not self._prompt("Terminate all instances?"):
      print "Not terminating cluster."
    else:
      print "Terminating cluster"
      self.cluster.terminate()
      
  def delete_cluster(self):
    self.cluster.delete()
    
  def create_formatted_snapshot(self, size, availability_zone,
                                image_id, key_name, ssh_options):
    Ec2Storage.create_formatted_snapshot(self.cluster, size,
                                         availability_zone,
                                         image_id,
                                         key_name,
                                         ssh_options)

  def _get_cluster_volumes(self, storage):
    if not os.path.isfile(storage._get_storage_filename()):
        instances = self.get_running_instances()
        if not instances:
            print "No running instances. If you are overriding a cluster it must be running for certain commands. Aborting."
            sys.exit(1)

        all_volumes = self.cluster.ec2Connection.get_all_volumes()
        return [(i.role, v) for v in all_volumes for i in instances if i.id == v.attach_data.instance_id]
    else:
        roles = storage.get_roles()
        all_volumes = self.cluster.ec2Connection.get_all_volumes()
        return [(r, v) for r in roles for v in all_volumes for mv in sum(storage._get_mountable_volumes(r),[]) if mv.volume_id == v.id]

  def _print_storage_status(self, storage):
    if not os.path.isfile(storage._get_storage_filename()):
      storage.print_status(volumes=self._get_cluster_volumes(storage))
    else:
      storage.print_status()

  def list_storage(self):
    storage = self.cluster.get_storage()
    self._print_storage_status(storage)

  def check_storage(self):
    storage = self.cluster.get_storage()
    storage.check() 

  def create_storage(self, role, number_of_instances,
                     availability_zone, spec_file):
    storage = self.cluster.get_storage()
    storage.create(role, number_of_instances, availability_zone, spec_file)
    self._print_storage_status(storage)
    
  def attach_storage(self, role):
    storage = self.cluster.get_storage()
    storage.attach(role, self.cluster.get_instances_in_role(role, 'running'))
    self._print_storage_status(storage)
    
  def delete_storage(self, force=False):
    storage = self.cluster.get_storage()
    self._print_storage_status(storage)
    if not force and not self._prompt("Delete all storage volumes? THIS WILL \
PERMANENTLY DELETE ALL DATA"):
      print "Not deleting storage volumes."
    else:
      print "Deleting storage"
      storage.delete(storage.get_roles())

  def snapshot_storage(self, cluster_name, force=False):
    storage = self.cluster.get_storage()

    if not force:
        self._print_storage_status(storage)
        if not force and not self._prompt("Create snapshots of ALL storage volumes?"):
          print "Not creating snapshots."
          return

    volumes = self._get_cluster_volumes(storage)
    storage.create_snapshot_of_all_volumes(cluster_name, volumes)

  def migrate_snapshots(self, cluster_name, new_zone, config_dir, force=False):
    json_file = os.path.join(config_dir, ".storage", "ec2-storage-%s.json" % cluster_name)
    
    if not os.path.isfile(json_file):
        print "Migrating storage requires that the storage mapping file exists for the cluster provided. Aborting."
        sys.exit(1)

    f = open(json_file, "r")
    map = simplejson.load(f)
    f.close()

    volume_ids_in_map = []

    all_devices = []
    for role, device_list in map.iteritems():
        for devices in device_list:
            for device in devices:
                all_devices.append(device)
                volume_ids_in_map.append(device['volume_id'])

    if not volume_ids_in_map:
        print "No volumes were found in storage mapping file. Aborting."
        sys.exit(1)

    ec2Connection = self.cluster.ec2Connection

    # check the volume_ids we found in the json map file to see if snapshots were generated from them for the
    # given cluster
    snapshots = filter(lambda x: x.volume_id in volume_ids_in_map, ec2Connection.get_all_snapshots(owner="self"))
    if len(snapshots) != len(volume_ids_in_map):
        print "Number of volumes in storage map (%d) does not match number of snaphots founds (%d). Abortings." % (len(volume_ids_in_map), len(snapshots))
        sys.exit(1)

    # check that we aren't migrating to the same availability zone
    volumes = ec2Connection.get_all_volumes(volume_ids_in_map)
    zone_set = set([v.zone for v in volumes])
    if len(zone_set) > 1:
        print "Found multiple availability zones in storage map volumes. Aborting."
        print zone_set
        sys.exit(1)

    old_zone = zone_set.pop()
    if old_zone == new_zone:
        print "Trying to migrate snapshots to the same availability zone '%s'. Aborting." % old_zone
        sys.exit(1)

    all_zones = ec2Connection.get_all_zones()
    for zone in all_zones:
        if new_zone == zone.name:
            break
    else:
        print "Target availability zone %s does not exist. Aborting." % new_zone
        sys.exit(1)

    if zone.state != 'available':
        print "The target availability zone is not available. Aborting."
        sys.exit(1)

    print "Creating %d new volume(s) in zone %s" % (len(snapshots), zone.name)

    for snapshot in snapshots:
        v = ec2Connection.create_volume(snapshot.volume_size, zone.name, snapshot)
        print "New volume %s created from snapshot %s" % (v.id, snapshot.id)
        for d in all_devices:
            if d['volume_id'] == snapshot.volume_id:
                d['volume_id'] = v.id

    # rename old storage mapping file
    json_file_backup = json_file+".%s" % datetime.isoformat(datetime.now())
    os.rename(json_file, json_file_backup)

    # write new storage mapping file
    f = open(json_file, "w")
    simplejson.dump(map, f, indent=2)
    f.close()

    print ""
    print "NOTE: You are now using a new storage mapping file for this zone."
    print "Your old storage mapping file has been renamed to %s" % json_file_backup
    
  def delete_snapshots(self, cluster_name, force=False):
    ec2 = self.cluster.ec2Connection
    snapshots = self.list_snapshots(cluster_name)

    if not snapshots:
        print "No snapshots found. Aborting"
        sys.exit(1)

    if not force:
        choice = raw_input("Delete %d snapshot(s) [yes or no]: " % len(snapshots))
        if choice != "yes":
            print "Not deleting snapshots."
            sys.exit(0)
        
    print "Deleting %d snapshots..." % len(snapshots)
    for s in snapshots:
        ec2.delete_snapshot(s.id)

  def list_snapshots(self, cluster_name):
    ec2 = self.cluster.ec2Connection
    snapshots = filter(lambda x: x.description.find(cluster_name) >= 0, ec2.get_all_snapshots(owner="self"))

    table = PrettyTable()
    table.set_field_names(("Snapshot", "Status", "Created", "Size", "Description"))
    for s in snapshots:
        table.add_row((s.id, s.status, s.start_time, s.volume_size, s.description))
    table.printt()

    return snapshots
  
  def login(self, ssh_options):
    raise Exception("Unimplemented")
    
  def proxy(self, ssh_options):
    raise Exception("Unimplemented")
    
  def push(self, ssh_options, file):
    raise Exception("Unimplemented")
    
  def execute(self, ssh_options, args):
    raise Exception("Unimplemented")
  
  def update_slaves_file(self, config_dir, ssh_options, private_key):
    raise Exception("Unimplemented")
  
  def _prompt(self, prompt):
    """ Returns true if user responds "yes" to prompt. """
    return raw_input("%s [yes or no]: " % prompt).lower() == "yes"

  def _call(self, command):
    print command
    try:
      subprocess.call(command, shell=True)
    except Exception, e:
      print e

  def _get_default_user_data_file_template(self):
    data_path = os.path.join(os.path.dirname(__file__), 'data')
    return os.path.join(data_path, '%s-%s-init-remote.sh' %
                 (self.get_service_code(), self.cluster.get_provider_code()))

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

    replacements = { "%ENV%": build_env_string(it.env_strings, {
      "ROLES": ",".join(it.roles),
      "USER_PACKAGES": it.user_packages,
      "AUTO_SHUTDOWN": it.auto_shutdown,
      "EBS_MAPPINGS": ";".join(ebs_mappings),
    }) }
    logging.info("EBS Mappings: %s" % ";".join(ebs_mappings))
    instance_user_data = InstanceUserData(user_data_file_template, replacements)

    instance_ids = self.cluster.launch_instances(it.roles, it.number, it.image_id,
                                            it.size_id,
                                            instance_user_data,
                                            key_name=it.key_name,
                                            public_key=it.public_key,
                                            placement=it.placement,
                                            security_groups=it.security_groups)

    logging.debug("Instance ids reported to start: %s" % str(instance_ids))
    return instance_ids

def get_service(service, provider):
    """
    Retrieve the Service class for a service and provider.
    """
    mod_name, service_classname = SERVICE_PROVIDER_MAP[service][provider]
    _mod = __import__(mod_name, globals(), locals(), [service_classname])
    return getattr(_mod, service_classname)
