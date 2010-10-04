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

from __future__ import with_statement

import ConfigParser
from boto.ec2.connection import EC2Connection
from cloud import VERSION
from cloud.cluster import get_cluster
from cloud.service import get_service
from cloud.service import InstanceTemplate
from cloud.settings import SERVICE_PROVIDER_MAP
from cloud.util import merge_config_with_options
from cloud.util import get_all_cluster_names_from_config_file 
from cloud.util import xstr
from optparse import OptionParser
from optparse import make_option
from prettytable import PrettyTable
from datetime import datetime
from dateutil.parser import parse as dateparser
from math import ceil
import re
import logging
import os
import urllib
import sys
import simplejson

DEFAULT_SERVICE_NAME = 'cassandra'
DEFAULT_CLOUD_PROVIDER = 'ec2'

DEFAULT_CONFIG_DIR_NAME = '.stratus'
DEFAULT_CONFIG_DIR = os.path.join(os.environ['HOME'], DEFAULT_CONFIG_DIR_NAME)
CONFIG_FILENAME = 'clusters.cfg'

VERSION_OPTION = \
  make_option("--version", action="store_true", default=False,
    help="Print the version and quit.")

CONFIG_DIR_OPTION = \
  make_option("--config-dir", metavar="CONFIG-DIR",
    help="The configuration directory.")

BASIC_OPTIONS = [
  CONFIG_DIR_OPTION,
]

LIST_OPTIONS = [
  CONFIG_DIR_OPTION,
  make_option("--override", action="store_true", default=False),
  make_option("--all", action="store_true", default=False, 
              help="list all clusters (defined or not)"),
]

SORT_OPTIONS = [
  make_option("-s", "--sort", metavar="COLUMN_NAME", help="Sort by COLUMN_NAME"),
  make_option("-r", "--reverse", metavar="REVERSE", help="Reverse sort order (true|false)"),
]

def print_usage(script):
  print """%(script)s CLUSTER COMMAND [OPTIONS]
where CLUSTER is a defined cluster name, COMMAND is a valid command
determined by the service and cloud provider types of the CLUSTER. 

Use %(script)s CLUSTER to see valid commands for the given CLUSTER 

------------------------------------------------------------------------------
The script also provides the following functionality:
  
%(script)s COMMAND [OPTIONS] 
where COMMAND may be one of the following:

  list (CLUSTER)                       list all defined clusters
                                         or details for CLUSTER
  
  list-storage                         list all volumes (EBS volumes in EC2 only)

Use %(script)s COMMAND --help to see additional options for specific commands.
""" % locals()

def print_deprecation(script, replacement):
  print "Deprecated. Use '%(script)s %(replacement)s'." % locals()

def parse_options_and_config(command, option_list=[], extra_arguments=(),
                             unbounded_args=False):
  """
  Parse the arguments to command using the given option list, and combine with
  any configuration parameters.

  If unbounded_args is true then there must be at least as many extra arguments
  as specified by extra_arguments (the first argument is always CLUSTER).
  Otherwise there must be exactly the same number of arguments as
  extra_arguments.
  """
  expected_arguments = ["CLUSTER",]
  expected_arguments.extend(extra_arguments)
  (options_dict, args) = parse_options(command, option_list, expected_arguments,
                                       unbounded_args)

  config_dir = get_config_dir(options_dict)
  config_files = [os.path.join(config_dir, CONFIG_FILENAME)]
  if 'config_dir' not in options_dict:
    # if config_dir not set, then also search in current directory
    config_files.insert(0, CONFIG_FILENAME)

  config = ConfigParser.ConfigParser()
  read_files = config.read(config_files)
  logging.debug("Read %d configuration files: %s", len(read_files),
                ", ".join(read_files))
  cluster_name = args[0]
  opt = merge_config_with_options(cluster_name, config, options_dict)
  logging.debug("Options: %s", str(opt))
  service_name = get_service_name(opt)
  cloud_provider = get_cloud_provider(opt)
  cluster = get_cluster(cloud_provider)(cluster_name, config_dir)
  service = get_service(service_name, cloud_provider)(cluster)
  return (opt, args, service)

def parse_options(command, option_list=[], expected_arguments=(),
                  unbounded_args=False):
  """
  Parse the arguments to command using the given option list.

  If unbounded_args is true then there must be at least as many extra arguments
  as specified by extra_arguments (the first argument is always CLUSTER).
  Otherwise there must be exactly the same number of arguments as
  extra_arguments.
  """

  config_file_name = "%s/%s" % (DEFAULT_CONFIG_DIR_NAME, CONFIG_FILENAME)
  usage = """%%prog %s [options] %s

Options may also be specified in a configuration file called
%s located in the user's home directory.
Options specified on the command line take precedence over any in the
configuration file.""" % (command, " ".join(expected_arguments),
                          config_file_name)
  parser = OptionParser(usage=usage, version="%%prog %s" % VERSION,
                        option_list=option_list)
  parser.disable_interspersed_args()
  (options, args) = parser.parse_args(sys.argv[2:])
  if unbounded_args:
    if len(args) < len(expected_arguments):
      parser.error("incorrect number of arguments")
  elif len(args) != len(expected_arguments):
    parser.error("incorrect number of arguments")
  return (vars(options), args)

def get_config_dir(options_dict):
  config_dir = options_dict.get('config_dir')
  if not config_dir:
    config_dir = DEFAULT_CONFIG_DIR
  return config_dir

def get_service_name(options_dict):
  service_name = options_dict.get("service", None)
  if service_name is None:
    service_name = DEFAULT_SERVICE_NAME
  return service_name

def get_cloud_provider(options_dict):
  provider = options_dict.get("cloud_provider", None)
  if provider is None:
    provider = DEFAULT_CLOUD_PROVIDER
  return provider

def check_options_set(options, option_names):
  for option_name in option_names:
    if options.get(option_name) is None:
      print "Option '%s' is missing. Aborting." % option_name
      sys.exit(1)

def get_image_id(cluster, options):
  if cluster.get_provider_code() == 'ec2':
    return options.get('image_id', options.get('ami'))
  else:
    return options.get('image_id')

def parse_options_for_delete_snapshots(command, option_list=[], unbounded_args=False):
    expected_arguments = ["CLUSTER"]
    (options_dict, args) = parse_options(command, option_list, expected_arguments,
                                       unbounded_args)
    return (options_dict, args)

def parse_options_for_migrate_snapshots(command, option_list=[], unbounded_args=False):
    expected_arguments = ["CLUSTER", "NEW_ZONE"]
    (options_dict, args) = parse_options(command, option_list, expected_arguments,
                                       unbounded_args)
    return (options_dict, args)

def parse_options_for_clusters(command, option_list=[], extra_arguments=(),
                             unbounded_args=True):

  expected_arguments = []
  (options_dict, args) = parse_options(command, option_list, expected_arguments,
                                       unbounded_args)

  config_dir = get_config_dir(options_dict)
  config_files = [os.path.join(config_dir, CONFIG_FILENAME)]
  if 'config_dir' not in options_dict:
    # if config_dir not set, then also search in current directory
    config_files.insert(0, CONFIG_FILENAME)

  config = ConfigParser.ConfigParser()
  read_files = config.read(config_files)
  logging.debug("Read %d configuration files: %s", len(read_files),
                ", ".join(read_files))

  clusters = {}
  for cluster_name in config.sections():
    clusters[cluster_name] = merge_config_with_options(cluster_name, config, options_dict)

  return (options_dict, args, clusters)

def getSortAndReverseOptions(opt, defaultSort="Size", defaultReverse=True):
    sort = opt.get('sort')
    if sort is None:
        sort = defaultSort

    reverse = opt.get('reverse')
    if reverse is None:
        reverse = defaultReverse
    elif reverse.lower() in ["true", "t"]:
        reverse = True
    elif reverse.lower() in ["false", "f"]:
        reverse = False
    else:
        reverse = defaultReverse

    return (sort, reverse)
    

def handleListStorage(command):
    (opt, args, _) = parse_options_for_clusters(command, SORT_OPTIONS)

    (sort, reverse) = getSortAndReverseOptions(opt)

    ec2 = EC2Connection()

    table = PrettyTable()
    table.set_field_names(["Volume Id", "Size", "Snapshot Id", "Availability Zone", "Status", "Create Time"])

    volumes = ec2.get_all_volumes()
    s = 0
    for v in volumes:
        s += v.size
        table.add_row([v.id, v.size, v.snapshot_id, v.zone, v.status, v.create_time])

    try:
        table.printt(sortby=sort, reversesort=reverse)
        print "Total volumes: %d" % len(table.rows)
        print "Total size:    %d (GB)" % s
    except ValueError, e:
        print "Sort error: %s" % str(e)
    

def handleListClusterDetails(cluster_name, config_dir):
    for (service_type, providers) in SERVICE_PROVIDER_MAP.iteritems():
      for cloud_provider in providers:
        service = get_service(service_type, cloud_provider)(None)
        cluster_names = service.get_clusters_for_provider(cloud_provider)
        for name in cluster_names:
          if name == cluster_name:
            cluster = get_cluster(cloud_provider)(cluster_name, config_dir)
            cluster.print_status()
            return
    print "No running instances."

def handleListClusters(command):
    (opt, args, defined_clusters) = parse_options_for_clusters(command, LIST_OPTIONS)
    config_dir = get_config_dir(opt)

    if len(args) > 0:
      handleListClusterDetails(args[0], config_dir)
      return

    rows = []
    for (cluster_name, params) in defined_clusters.iteritems():
      cloud_provider = params['cloud_provider']
      service_type = params['service_type']
      cluster = get_cluster(cloud_provider)(cluster_name, config_dir)
      service = get_service(service_type, cloud_provider)(cluster)

      running_instances = service.get_running_instances()
      total_time = getTotalClusterRunningTime(running_instances)
      instance_type = running_instances[0].instance_type if running_instances else "N/A"
      rows.append([cluster_name, service_type, cloud_provider, len(running_instances), 
                   total_time, instance_type, "*"])

    if opt['all'] == True:
      handleListAllCommand(rows, config_dir, defined_clusters.keys())

    rows.sort(key = lambda x: x[3], reverse=True)

    table = PrettyTable()
    table.set_field_names(("", "Cluster Name", "Service", "Cloud Provider", "Instances", 
                           "Cluster Time (hrs)", "Instance Type", "Defined"))

    for i in xrange(len(rows)):
        rows[i].insert(0, i+1)
        table.add_row(rows[i])

    table.printt()
    print "Running instances: %d" % sum(map(lambda x: x[4], table.rows))

    while True:
      try:
        choice = raw_input("View details for cluster [Enter to quit]: ")
        if choice == "":
          return
        choice = int(choice)
        if choice > len(table.rows):
          print "Not a valid choice. Try again."
        else:
          r = table.rows[choice-1]
          print "\nDetails for cluster: %s" % r[1]

          config_dir = get_config_dir(opt)
          cluster = get_cluster(r[3])(r[1], config_dir)
          service = get_service(r[2], r[3])(cluster)
          service.list()
      except ValueError:
        print "Not a valid choice. Try again."
      except KeyboardInterrupt:
        print
        return

def handleListAllCommand(rows, config_dir, defined_clusters=[]):

  for (service_type, providers) in SERVICE_PROVIDER_MAP.iteritems():
    for cloud_provider in providers:
      service = get_service(service_type, cloud_provider)(None)
      cluster_names = service.get_clusters_for_provider(cloud_provider)

      for name in cluster_names:
        if name in defined_clusters:
          continue
        cluster = get_cluster(cloud_provider)(name, config_dir)
        service = get_service(service_type, cloud_provider)(cluster)
        running_instances = service.get_running_instances()
        total_time = getTotalClusterRunningTime(running_instances)
        instance_type = running_instances[0].instance_type if running_instances else "N/A"
        rows.append([name, service_type, cloud_provider, len(running_instances), total_time, instance_type, ""])

def getTotalClusterRunningTime(instances):
  now = datetime.utcnow()
  return sum([int(ceil((now - dateparser(i.launch_time.split(".")[0])).total_seconds()/3600.0)) for i in instances])

def getServiceTypeAndCloudProvider(cluster_name):
  for (service_type, providers) in SERVICE_PROVIDER_MAP.iteritems():
    for cloud_provider in providers:
      service = get_service(service_type, cloud_provider)(None)
      cluster_names = service.get_clusters_for_provider(cloud_provider)
      
      for name in cluster_names:
        if name == cluster_name:
          return service_type, cloud_provider
  return None, None
    
def handleClusterSpecificCommands(cluster_name, cluster_params, opt, args, config_dir):
  if cluster_params is None:
    service_type, cloud_provider = getServiceTypeAndCloudProvider(cluster_name)
    if service_type is None or cloud_provider is None:
      print "Unable to lookup servie type or cloud provider for cluster: %s. Aborting." % cluster_name
      sys.exit(1)
  else:
    service_type = cluster_params.get("service_type")
    cloud_provider = cluster_params.get("cloud_provider")

  command = args[0] if args else ""
  argv = args[1:] if args else []

  # cluster name follows arguments
  i = -1
  for arg in argv:
    if arg.startswith("-"):
      i += 1
    else:
      break
  argv.insert(i+1, cluster_name)

  _mod = __import__("%s.cli" % service_type, globals(), locals(), "execute")
  getattr(_mod, "execute")(command, argv)

def main():
  if len(sys.argv) < 2:
    print_usage(sys.argv[0])
    sys.exit(1)

  if "--version" in sys.argv:
      print "Stratus version: %s" % VERSION
      sys.exit(0)

  command_or_cluster = sys.argv[1]

  if command_or_cluster == 'list':
    handleListClusters(command_or_cluster)
  elif command_or_cluster == 'list-storage':
    handleListStorage(command_or_cluster)
  else:
    (opt, args, defined_clusters) = parse_options_for_clusters(command_or_cluster, LIST_OPTIONS)
    config_dir = get_config_dir(opt)

    if command_or_cluster in defined_clusters:
      handleClusterSpecificCommands(command_or_cluster, defined_clusters[command_or_cluster], opt, args, config_dir)
      return
    elif opt['override']:
      handleClusterSpecificCommands(command_or_cluster, None, opt, args, config_dir)
      return

    print "Unrecognized command or undefined cluster '%s'" % command_or_cluster
    print_usage(sys.argv[0])
    sys.exit(1)
