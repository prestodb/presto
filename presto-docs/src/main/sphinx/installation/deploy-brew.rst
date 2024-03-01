============================================
Deploy Presto on a Mac using Homebrew
============================================

- If you are deploying Presto on an Intel Mac, see `Deploy Presto on an Intel Mac using Homebrew`_.

- If you are deploying Presto on an Apple Silicon Mac that has an M1 or M2 chip, see `Deploy Presto on an Apple Silicon Mac using Homebrew`_. 

Deploy Presto on an Intel Mac using Homebrew
--------------------------------------------
*Note*: These steps were developed and tested on Mac OS X on Intel. These steps will not work with Apple Silicon (M1 or M2) chips.

Following these steps, you will:

- install the Presto service and CLI on an Intel Mac using `Homebrew <https://formulae.brew.sh/formula/prestodb#default>`_
- start and stop the Presto service
- start the Presto CLI

Install Presto
^^^^^^^^^^^^^^

Follow these steps to install Presto on an Intel Mac using `Homebrew <https://formulae.brew.sh/formula/prestodb#default>`_. 

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

For example, the full path to the node.properties file is */usr/local/Cellar/prestodb/<version>/libexec/etc/node.properties*. 

The Presto CLI is installed in the *bin* directory of the Presto install directory: */usr/local/Cellar/prestodb/<version>/bin*.

The executables are added to */usr/local/bin* path and should be available as part of $PATH.

Start and Stop Presto
^^^^^^^^^^^^^^^^^^^^^

To start Presto, use the ``presto-server`` helper script. 

To start the Presto service in the background, run the following command: 

``presto-server start``

To start the Presto service in the foreground, run the following command:

``presto-server run``

To stop the Presto service in the background, run the following command:

``presto-server stop``

To stop the Presto service in the foreground, close the terminal or select Ctrl + C until the terminal prompt is shown. 

Access the Presto Web Console
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

After starting Presto, you can access the web UI using the following link in a browser:

``http://localhost:8080``

*Note*: The default port is 8080. To configure the Presto service to use a different port see `Config Properties <deployment.html#config-properties>`_.

.. figure:: ../images/presto_console.png
   :align: center

Start the Presto CLI
^^^^^^^^^^^^^^^^^^^^

The Presto CLI is a terminal-based interactive shell for running queries, and is a
`self-executing <http://skife.org/java/unix/2011/06/20/really_executable_jars.html>`_
JAR file that acts like a normal UNIX executable.

The Presto CLI is installed in the *bin* directory of the Presto install directory: */usr/local/Cellar/prestodb/<version>/bin*.

To run the Presto CLI, run the following command:

``presto``

The Presto CLI starts and displays the prompt ``presto>``. 

For more information on the Presto CLI, see :doc:`Command Line Interface <cli>`.

Deploy Presto on an Apple Silicon Mac using Homebrew 
----------------------------------------------------
*Note*: These steps were developed and tested on Mac OS X on Apple Silicon. These steps will not work with Intel chips.

Following these steps, you will:

- install the Presto service and CLI on an Apple Silicon Mac using `Homebrew <https://formulae.brew.sh/formula/prestodb#default>`_
- start and stop the Presto service
- start the Presto CLI

Install Presto
^^^^^^^^^^^^^^

Follow these steps to install Presto on an Apple Silicon Mac using `Homebrew <https://formulae.brew.sh/formula/prestodb#default>`_. 

1. If you do not have brew installed, run the following command:

   ``arch -x86_64 /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install.sh)"``

   This installs Homebrew into ``/usr/local/bin``. 
   
   *Note*: The default installation of Homebrew on Apple Silicon is to ``/opt/homebrew``.

2. To allow the shell to look for Homebrew in ``/usr/local/bin`` before it looks for Homebrew in ``/opt/homebrew``, run the following command:

   ``export PATH=/usr/local/bin:$PATH``

3. To install Presto, run the following command:

   ``arch -x86_64 brew install prestodb``

   Presto is installed in the directory */usr/local/Cellar/prestodb/<version>.* The executables ``presto`` 
   and ``presto-server`` are installed in ``/usr/local/bin/``.

The following files are created in the *libexec/etc* directory in the Presto install directory:

- node.properties
- jvm.config
- config.properties
- log.properties
- catalog/jmx.properties

For example, the full path to the node.properties file is */usr/local/Cellar/prestodb/<version>/libexec/etc/node.properties*. 

The Presto CLI is installed in the *bin* directory of the Presto install directory: */usr/local/Cellar/prestodb/<version>/bin*.

The executables are added to */usr/local/bin* path and should be available as part of $PATH.

Start and Stop Presto
^^^^^^^^^^^^^^^^^^^^^

To start Presto, use the ``presto-server`` helper script. 

To start the Presto service in the background, run the following command: 

``arch -x86_64 presto-server start``

To start the Presto service in the foreground, run the following command:

``arch -x86_64 presto-server run``

To stop the Presto service in the background, run the following command:

``presto-server stop``

To stop the Presto service in the foreground, close the terminal or select Ctrl + C until the terminal prompt is shown. 

Access the Presto Web Console
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

After starting Presto, you can access the web UI using the following link in a browser:

``http://localhost:8080``

*Note*: The default port is 8080. To configure the Presto service to use a different port see `Config Properties <deployment.html#config-properties>`_.

.. figure:: ../images/presto_console.png
   :align: center

Start the Presto CLI
^^^^^^^^^^^^^^^^^^^^

The Presto CLI is a terminal-based interactive shell for running queries, and is a
`self-executing <http://skife.org/java/unix/2011/06/20/really_executable_jars.html>`_
JAR file that acts like a normal UNIX executable.

The Presto CLI is installed in the *bin* directory of the Presto install directory: */usr/local/Cellar/prestodb/<version>/bin*.
The executable ``presto`` is installed in ``/usr/local/bin/``.

To run the Presto CLI, run the following command:

``presto``

The Presto CLI starts and displays the prompt ``presto>``. 

For more information on the Presto CLI, see :doc:`Command Line Interface <cli>`.