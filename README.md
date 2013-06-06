This work was originally derived from the Cloudera CDH Cloud Scripts for managing Hadoop in 
Amazon EC2 (https://wiki.cloudera.com/display/DOC/CDH+Cloud+Scripts). We needed a way to 
manage Hadoop, Cassandra, and other distributed services, thus PyStratus was born. Thanks 
to Cloudera for providing a great starting point for us!Currently only Amazon EC2 is supported, 
but we hope to add new cloud providers very soon.

To get up and running quickly, use virtualenv and install PyStratus with these instructions: 
```code
$ mkvirtualenv stratus
(stratus)$ pip install https://github.com/digitalreasoning/PyStratus/archive/master.zip
...
# issue commands like: 
(stratus)$ stratus list
(stratus)$ stratus exec HADOOP_CLUSTER launch-cluster 3
(stratus)$ stratus exec HADOOP_CLUSTER terminate-cluster
...
(stratus)$ deactivate # to leave virtualenv
```


Additionally, the following script is sufficient (assumes that you have a ~/bin directory and it is on your PATH):

```code
INSTALL_DIR=~/Tools/pystratus
virtualenv $INSTALL_DIR --no-site-packages
$INSTALL_DIR/bin/pip install https://github.com/digitalreasoning/PyStratus/archive/master.zip
ln -snf $INSTALL_DIR/bin/stratus ~/bin/stratus
```

PyStratus uses the following dependencies:

* Python 2.5+
* boto 
* simplejson
* prettytable
* setuptools
* dateutil
* PyYAML
* cElementTree or elementree
* Fabric

You may also check out the project and run "python setup.py install" and the command "stratus" will now available 
and an egg file will be located in your site-packages directory. You may want to run the command with 
sudo to install it for all users.

See the full documentation at http://github.com/digitalreasoning/PyStratus/wiki/Documentation


