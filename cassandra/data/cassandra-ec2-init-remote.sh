#!/bin/bash -x

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

################################################################################
# Script that is run on each EC2 instance on boot. It is passed in the EC2 user
# data, so should not exceed 16K in size after gzip compression.
#
# This script is executed by /etc/init.d/ec2-run-user-data, and output is
# logged to /var/log/messages.
################################################################################

set -e -x

################################################################################
# Initialize variables
################################################################################

# Substitute environment variables passed by the client
export %ENV%

# Write environment variables to /root/.bash_profile
echo "export %ENV%" >> ~root/.bash_profile
echo "export %ENV%" >> ~root/.bashrc

DEFAULT_CASSANDRA_URL="http://mirror.cloudera.com/apache/cassandra/0.6.4/apache-cassandra-0.6.4-bin.tar.gz"
CASSANDRA_HOME_ALIAS=/usr/local/apache-cassandra

function install_cassandra() {

    curl="curl --retry 3 --silent --show-error --fail"
    if [ ! -z "$CASSANDRA_URL" ]; then
        DEFAULT_CASSANDRA_URL=$CASSANDRA_URL
    fi

    cassandra_tar_file=`basename $DEFAULT_CASSANDRA_URL`
    $curl -O $DEFAULT_CASSANDRA_URL
    
    tar zxf $cassandra_tar_file -C /usr/local
    rm -f $cassandra_tar_file

    CASSANDRA_HOME_WITH_VERSION=/usr/local/`ls -1 /usr/local | grep cassandra`

    echo "export CASSANDRA_HOME=$CASSANDRA_HOME_ALIAS" >> ~root/.bash_profile
    echo 'export PATH=$CASSANDRA_HOME/bin:$PATH' >> ~root/.bash_profile
}

function wait_for_mount {
  mount=$1
  device=$2

  mkdir -p $mount

  i=1
  echo "Attempting to mount $device"
  while true ; do
    sleep 10
    echo -n "$i "
    i=$[$i+1]
    mount -o defaults,noatime $device $mount || continue
    echo " Mounted."
    break;
  done

  if [ -e $mount/lost+found ]; then
    rm -rf $mount/lost+found
  fi
}

function configure_cassandra() {
  if [ -n "$EBS_MAPPINGS" ]; then
    # EBS_MAPPINGS is like "cn,/ebs1,/dev/sdj;cn,/ebs2,/dev/sdk"
    # EBS_MAPPINGS is like "ROLE,MOUNT_POINT,DEVICE;ROLE,MOUNT_POINT,DEVICE"
    for mapping in $(echo "$EBS_MAPPINGS" | tr ";" "\n"); do
      role=`echo $mapping | cut -d, -f1`
      mount=`echo $mapping | cut -d, -f2`
      device=`echo $mapping | cut -d, -f3`
      wait_for_mount $mount $device
    done
  fi

  # configure the cassandra.in.sh script based on instance type
  INSTANCE_TYPE=`wget -q -O - http://169.254.169.254/latest/meta-data/instance-type`
  SETTINGS_FILE=$CASSANDRA_HOME_WITH_VERSION/bin/cassandra.in.sh

  cat > $SETTINGS_FILE <<EOF
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cassandra_home=$CASSANDRA_HOME_ALIAS

# The directory where Cassandra's configs live (required)
CASSANDRA_CONF=\$cassandra_home/conf

# This can be the path to a jar file, or a directory containing the 
# compiled classes. NOTE: This isn't needed by the startup script,
# it's just used here in constructing the classpath.
cassandra_bin=\$cassandra_home/build/classes
#cassandra_bin=\$cassandra_home/build/cassandra.jar

# JAVA_HOME can optionally be set here
#JAVA_HOME=/usr/local/jdk6

# The java classpath (required)
CLASSPATH=\$CASSANDRA_CONF:\$cassandra_bin

for jar in \$cassandra_home/lib/*.jar; do
    CLASSPATH=\$CLASSPATH:\$jar
done

EOF

  case $INSTANCE_TYPE in
  m1.xlarge|c1.xlarge)
    cat >> $SETTINGS_FILE <<EOF
# Arguments to pass to the JVM
JVM_OPTS="-ea -Xms10G -Xmx10G -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+CMSParallelRemarkEnabled -XX:SurvivorRatio=8 -XX:MaxTenuringThreshold=1 -XX:+HeapDumpOnOutOfMemoryError -Dcom.sun.management.jmxremote.port=8080 -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false"
EOF
    ;;
  *)
    cat >> $SETTINGS_FILE <<EOF
# Arguments to pass to the JVM
JVM_OPTS="-ea -Xms256M -Xmx1G -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+CMSParallelRemarkEnabled -XX:SurvivorRatio=8 -XX:MaxTenuringThreshold=1 -XX:+HeapDumpOnOutOfMemoryError -Dcom.sun.management.jmxremote.port=8080 -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false"
EOF
    ;;
  esac
}

function finish_cassandra {
    # symlink the actual cassandra directory to a more standard one (without version)
    # this is very important so the cluster can be configured more easily after the 
    # machines start and services started/stopped
    #
    # NOTE: Stratus also looks for this aliased directory to know when cassandra
    # is ready to be started
    ln -s $CASSANDRA_HOME_WITH_VERSION $CASSANDRA_HOME_ALIAS
}

install_cassandra
configure_cassandra
finish_cassandra
