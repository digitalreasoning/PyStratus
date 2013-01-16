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
                     security_groups=[], spot_config=None):
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
    self.spot_config = spot_config

    t = type(self.security_groups)
    if t is types.NoneType:
        self.security_groups = []
    elif t is types.StringType:
        self.security_groups = [security_groups]

  def add_env_strings(self, env_strings):
    new_env_strings = list(self.env_strings or [])
    new_env_strings.extend(env_strings)
    self.env_strings = new_env_strings

def get_service(service, provider):
    """
    Retrieve the Service class for a service and provider.
    """
    mod_name, service_classname = SERVICE_PROVIDER_MAP[service][provider]
    _mod = __import__(mod_name, globals(), locals(), [service_classname])
    return getattr(_mod, service_classname)
