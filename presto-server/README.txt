# Presto

Presto is an open source distributed SQL query engine for running
interactive analytic queries against data sources of all sizes ranging
from gigabytes to petabytes.

For an overview of Presto see the Overview section of the Presto
project's web site:
http://prestodb.io/docs/current/installation/deployment.html

## Requirements

* Java 7, 64-bit
* Python 2.4+ (for running Presto with the launch script)

### Deploying Presto

A Presto installation consists of two types of nodes - Presto
coordinators and Presto workers.  While it is possible to deploy
Presto on a single node running both components, it is more common to
run Presto across a cluster of instances to take advantage of Presto's
ability to distribute queries across multiple nodes.

For more information about deploying Presto, see the Deploying Presto
section of Presto's documentation:
http://prestodb.io/docs/current/installation/deployment.html

### Configuring Presto

Before running Presto, you will need to configure your Presto
instance. It is recommended that you create a data directory outside
of this Presto installation directory to allow for easier upgrades
when new versions of Presto are released.

Three configuration files need to be created to configure a Presto node:

* etc/node.properties - Contains a node's unique id and data directory

* etc/jvm.config - Configures JVM options including heap and memory
  options

* etc/config.properties -Contains presto configuration including node
  type, which port Presto listens on, and what discovery server to
  use.

Smaple configuration for each of the configuration files listed above
can be found in the Deployment section of Presto's documentation:
http://prestodb.io/docs/current/installation/deployment.html

### Running Presto

Once Presto is configured it can be started with the launcher script
in bin/. To start Presto as a daemon run:

    bin/launcher start

To start Presto in the foreground, run:

    bin/launcher run

After starting Presto logs will be available in var/log.

### Running the Presto CLI

This distribution contains the server-side components of Presto
capable of running either a Presto coordinator or a Presto worker. The
Presto project also distributes a Presto CLI - a terminal-based
interactive shell for running queries.

To download and install the Presto CLI, refer to the Command Line
Interface section of the Presto deployment guide:
http://prestodb.io/docs/current/installation/deployment.html


