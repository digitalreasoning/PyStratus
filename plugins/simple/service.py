import os
import sys
import time
import subprocess
import urllib
import tempfile

from cloud.cluster import TimeoutException
from cloud.service import InstanceTemplate
from cloud.plugin import ServicePlugin 
from cloud.util import xstr

from yaml import load as parse_yaml
from yaml import dump as dump_yaml

try:
    from cElementTree import parse as parse_xml
    from cElementTree import tostring as dump_xml
    from cElementTree import Element
except:
    try:
        from xml.etree.cElementTree import parse as parse_xml
        from xml.etree.cElementTree import tostring as dump_xml
        from xml.etree.cElementTree import Element
    except:
        print "*"*80
        print "WARNING: cElementTree module does not exist. Defaulting to elementtree instead."
        print "It's recommended that you install the cElementTree module for faster XML parsing."
        print "*"*80
        from elementtree.ElementTree import parse as parse_xml
        from elementtree.ElementTree import parse as parse_xml
        from elementtree.ElementTree import Element

class SimpleService(ServicePlugin):
    """
    """
    SIMPLE_NODE = "sn"

    def __init__(self):
        super(SimpleService, self).__init__()

    def get_roles(self):
        return [self.SIMPLE_NODE]

    def get_instances(self):
        return self.cluster.get_instances_in_role(self.SIMPLE_NODE, "running")

    def _wait_for_install(self, instance, ssh_options, wait_dir):
        """
        Simply wait for the 'wait' directory to be available so that we can begin configuring
        the service before starting it
        """
        wait_time = 3
        command = "ls %s" % wait_dir
        ssh_command = self._get_standard_ssh_command(instance, ssh_options, command)

        self.logger.info("Waiting for install with command %s" % ssh_command)
        while True:
            retcode = subprocess.call(ssh_command, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            if retcode == 0:
                break
            self.logger.debug("Sleeping for %d seconds..." % wait_time)
            time.sleep(wait_time)

    def expand_cluster(self, instance_template, ssh_options, wait_dir):
        instances = self.get_instances()

        instance_ids = self._launch_instances(instance_template)

        if len(instance_ids) != instance_template.number:
            self.logger.warn("Number of reported instance ids (%d) " \
                             "does not match requested number (%d)" % \
                             (len(instance_ids), instance_template.number))
        self.logger.debug("Waiting for %s instance(s) to start: %s" % \
            (instance_template.number, ", ".join(instance_ids)))
        time.sleep(1)

        try:
            self.cluster.wait_for_instances(instance_ids)
            self.logger.debug("%d instances started" % (instance_template.number,))
        except TimeoutException:
            self.logger.error("Timeout while waiting for %s instance to start." % \
                ",".join(instance_template.roles))

        instances = self.get_instances()
        self.logger.debug("We have %d current instances...", len(instances))
        new_instances = [instance for instance in instances if instance.id in instance_ids]
        if(len(new_instances) != len(instance_ids)) :
            raise Exception("Could only find %d new instances, expected %s" % (len(new_instances), str(instance_ids)))

        for instance in instances:
            self._wait_for_install(instance, ssh_options, wait_dir)
        self.logger.info("Instances started: %s" % (str(new_instances),))

        self._attach_storage(instance_template.roles)


    def launch_cluster(self, instance_template, ssh_options, wait_dir):
        """
        """
        if self.get_instances() :
            raise Exception("This cluster is already running.  It must be terminated prior to being launched again.")

        self.expand_cluster(instance_template, ssh_options, wait_dir)

    def login(self, instance, ssh_options):
        ssh_command = self._get_standard_ssh_command(instance, ssh_options)
        subprocess.call(ssh_command, shell=True)
