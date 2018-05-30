# Presto RPM

## RPM Package Build And Usage

You can build an RPM package for Presto server and install Presto using the RPM. Thus, the installation is easier to manage on RPM-based systems.

The RPM builds by default in Maven, and can be found under the directory `presto-server-rpm/target/`

The RPM has a pre-requisite of Python >= 2.4. It also needs Oracle Java 1.8 update 40 (8u40 64-bit) pre-installed. The RPM installation will fail if any of these requirements are not
satisfied.

To install Presto using an RPM, run:

    rpm -i presto-server-<version>-1.0.x86_64.rpm

This will install Presto in single node mode, where both coordinator and workers are co-located on localhost. This will deploy the necessary default configurations along with a service script to control the Presto server process.

Uninstalling the RPM is like uninstalling any other RPM, just run:

    rpm -e presto

Note: During uninstall, any Presto related files deployed will be deleted except for the Presto logs directory `/var/log/presto`.

## Control Scripts

The Presto RPM will also deploy service scripts to control the Presto server process. The script is configured with chkconfig,
so that the service can be started automatically on OS boot. After installing Presto from the RPM, you can run:

    service presto [start|stop|restart|status]

## Installation directory structure

We use the following directory structure to deploy various Presto artifacts.

* /usr/lib/presto/lib/: Various libraries needed to run the product. Plugins go in a plugin subdirectory.
* /etc/presto: General Presto configuration files like node.properties, jvm.config, config.properties. Connector configs go in a catalog subdirectory
* /etc/presto/env.sh: Java installation path used by Presto
* /var/log/presto: Log files
* /var/lib/presto/data: Data directory
* /usr/shared/doc/presto: Docs
* /etc/rc.d/init.d/presto: Control script

The node.properties file requires the following two additional properties since our directory structure is different from what standard Presto expects.

    catalog.config-dir=/etc/presto/catalog
    plugin.dir=/usr/lib/presto/lib/plugin
