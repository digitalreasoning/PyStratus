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
Utility functions.
"""

import os
import csv
import time
import ConfigParser
import socket
from subprocess import Popen, PIPE, CalledProcessError
import urllib2

from boto.ec2 import regions as EC2Regions

def get_ec2_connection(regionName):
    for region in EC2Regions():
        if region.name == regionName:
            return region.connect()

    raise RuntimeError("Unknown region name: %s" % regionName)

def bash_quote(text):
  """Quotes a string for bash, by using single quotes."""
  if text == None:
    return ""
  return "'%s'" % text.replace("'", "'\\''")

def bash_quote_env(env):
  """Quotes the value in an environment variable assignment."""
  if env.find("=") == -1:
    return env
  (var, value) = env.split("=", 1)
  return "%s=%s" % (var, bash_quote(value))

def build_env_string(env_strings=[], pairs={}):
  """Build a bash environment variable assignment"""
  env = ''
  if env_strings:
    for env_string in env_strings:
      env += "%s " % bash_quote_env(env_string)
  if pairs:
    for key, val in pairs.items():
      env += "%s=%s " % (key, bash_quote(val))
  return env[:-1]

def get_all_cluster_names_from_config_file(config):
  return config.sections()

def merge_config_with_options(section_name, config, options):
  """
  Merge configuration options with a dictionary of options.
  Keys in the options dictionary take precedence.
  """
  res = {}
  try:
    for (key, value) in config.items(section_name):
      if value.find("\n") != -1:
        res[key] = value.split("\n")
      else:
        res[key] = value
  except ConfigParser.NoSectionError:
    pass
  except ValueError, e:
    # incomplete format error usually means you forgot
    # to include the type for interpolation
    if "incomplete format" in e.message:
       msg = "Section '%s'. Double check that your formatting " \
             "contains the format type after the closing parantheses. " \
             "Example: %%(foo)s" % section_name
       raise ConfigParser.InterpolationError(options, section_name, msg)

  for key in options:
    if options[key] != None:
      res[key] = options[key]
  return res

def url_get(url, timeout=10, retries=0):
  """
  Retrieve content from the given URL.
  """
   # in Python 2.6 we can pass timeout to urllib2.urlopen
  socket.setdefaulttimeout(timeout)
  attempts = 0
  while True:
    try:
      return urllib2.urlopen(url).read()
    except urllib2.URLError:
      attempts = attempts + 1
      if attempts > retries:
        raise

def xstr(string):
  """Sane string conversion: return an empty string if string is None."""
  return '' if string is None else str(string)

def check_output(*popenargs, **kwargs):
  r"""Run command with arguments and return its output as a byte string.

  If the exit code was non-zero it raises a CalledProcessError.  The
  CalledProcessError object will have the return code in the returncode
  attribute and output in the output attribute.

  The arguments are the same as for the Popen constructor.  Example:

  >>> check_output(["ls", "-l", "/dev/null"])
  'crw-rw-rw- 1 root root 1, 3 Oct 18  2007 /dev/null\n'

  The stdout argument is not allowed as it is used internally.
  To capture standard error in the result, use stderr=STDOUT.

  >>> check_output(["/bin/sh", "-c",
  ...               "ls -l non_existent_file ; exit 0"],
  ...              stderr=STDOUT)
  'ls: non_existent_file: No such file or directory\n'

  NOTE: copied from 2.7 standard library so that we maintain our compatibility with 2.5
  """
  if 'stdout' in kwargs:
      raise ValueError('stdout argument not allowed, it will be overridden.')
  process = Popen(stdout=PIPE, *popenargs, **kwargs)
  output, unused_err = process.communicate()
  retcode = process.poll()
  if retcode:
      cmd = kwargs.get("args")
      if cmd is None:
          cmd = popenargs[0]
      raise CalledProcessError(retcode, cmd)
  return output

def log_cluster_action(config_dir, cluster_name, command, number,
instance_type=None, provider=None, plugin=None):
    """Log details of cluster launching or termination to a csv file.
    """

    csv_file = open(os.path.join(config_dir, "launch_log.csv"), "a+b")
    csv_log = csv.writer(csv_file)
    csv_log.writerow([cluster_name, command, number, instance_type, provider, plugin, time.strftime("%Y-%m-%d %H:%M:%S %Z")])
    csv_file.close()
