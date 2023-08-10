============================================
Deploy Presto on an Intel Mac using Homebrew
============================================

*Note*: These steps were developed and tested on Mac OS X on Intel. These steps will not work with Apple Silicon CPUs.

Following these steps, you will:

- install the Presto service and CLI on an Intel Mac using `Homebrew <https://formulae.brew.sh/formula/prestodb#default>`_
- start and stop the Presto service
- start the Presto CLI

Install Presto
==============

Follow these steps to install Presto on an Intel CPU Mac using `Homebrew <https://formulae.brew.sh/formula/prestodb#default>`_. 

1. If you do not have brew installed, run the following command:

   ``/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install.sh)"``

2. To install Presto, run the following command:

   ``brew install prestodb``

   Presto is installed in the directory */usr/local/Cellar/prestodb/<version>.* 

The following files are created in the *libexec/etc* directory in the Presto install directory:

- node.properties
- jvm.config
- config.properties
- log.properties
- catalog/jmx.properties

For example, the full path to the node.properties file is */usr/local/Cellar/prestodb/0.282/libexec/etc/node.properties* 

The Presto CLI is installed in the *bin* directory of the Presto install directory: */usr/local/Cellar/prestodb/<version>/bin*.

The executables are added to */usr/local/bin* path and should be available as part of $PATH.

Start and Stop Presto
=====================

To start Presto, use the presto-server helper script. 

To start the Presto service in the background, run the following command: 

``presto-server start``

To start the Presto service in the foreground, run the following command:

``presto-server run``

To stop the Presto service in the background, run the following command:

``presto-server stop``

To stop the Presto service in the foreground, close the terminal or select Ctrl + C until the terminal prompt is shown. 

Access the Presto Web Console
=============================

After starting Presto, you can access the web UI using the following link in a browser:

``http://localhost:8080``

*Note*: The default port is 8080. To configure the Presto service to use a different port see `Config Properties <deployment.html#config-properties>`_.

.. figure:: ../images/presto_console.png
   :align: center

Start the Presto CLI
====================

The Presto CLI is a terminal-based interactive shell for running queries, and is a
`self-executing <http://skife.org/java/unix/2011/06/20/really_executable_jars.html>`_
JAR file that acts like a normal UNIX executable.

The Presto CLI is installed in the *bin* directory of the Presto install directory: */usr/local/Cellar/prestodb/<version>/bin*.

To run the Presto CLI and use it to send SQL queries to a local or a remote Presto service, see :doc:`Command Line Interface <cli>`.
