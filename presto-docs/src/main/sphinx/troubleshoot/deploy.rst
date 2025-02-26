================
Deploying Presto
================

This page presents common problems encountered when deploying Presto. 

.. contents::
    :local:
    :backlinks: none
    :depth: 1

``Permission denied: ‘/private/var/presto/data/var/run/launcher.pid``
---------------------------------------------------------------------

Problem
^^^^^^^

``bin/launcher run`` or ``bin/launcher start`` returns the following error: 

``Permission denied: ‘/private/var/presto/data/var/run/launcher.pid``

Solution
^^^^^^^^

Run the command as root or using ``sudo``. 

.. code-block:: none

    sudo bin/launcher run
    sudo bin/launcher start


``xcode select failed to locate python``
----------------------------------------

Problem
^^^^^^^

(OS X only) ``bin/launcher run`` or ``bin/launcher start`` returns the following error: 

``xcode select failed to locate python requesting installation of command``

Solution
^^^^^^^^

Create a symlink where XCode looks for python that links to python3. An example of such a command: 

.. code-block:: none

    ln -s /Library/Developer/CommandLineTools/usr/bin/python3 /Library/Developer/CommandLineTools/usr/bin/python




``Error: VM option ‘UseG1GC’ is experimental``
----------------------------------------------

Problem
^^^^^^^
``bin/launcher run`` or ``bin/launcher start`` returns the following error: 

.. code-block:: none

    Error: VM option ‘UseG1GC’ is experimental and must be enabled via -XX:+UnlockExperimentalVM Options.

    Error: Could not create the Java Virtual Machine.

    Error: A fatal exception has occurred. Program will exit.

Solution
^^^^^^^^

This error occurs with some versions of Java. 

1. Check the version of Java that is installed on the system. 

2. If the installed version of Java is not the version in 
   `Requirements <https://github.com/prestodb/presto?tab=readme-ov-file#requirements>`_, 
   then uninstall Java and install a recommended version.