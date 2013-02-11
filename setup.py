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

from setuptools import setup, find_packages

version = __import__('cloud').VERSION

setup(name='stratus',
    version=version,
    description='Scripts for running various services on cloud providers',
    license = 'Apache License (2.0)',
    author = 'Abe Music - Digital Reasoning Systems, Inc.',
    author_email = 'abe.music@digitalreasoning.com',
    packages=['cloud', 'cloud.providers', 'cloud.plugins', 'cloud.plugins.cassandra', 'cloud.plugins.hadoop', 'cloud.plugins.hadoop_cassandra_hybrid', 'cloud.plugins.simple'],
    package_dir = {'cloud.plugins': 'plugins'},
    scripts=['stratus'],
    include_package_data=True,
    package_data = {'': ['*.plugin']},
    install_requires = ['boto==2.0','python-dateutil==1.5','simplejson','prettytable==0.5','yapsy==1.8','fabric','PyYAML'],
)
