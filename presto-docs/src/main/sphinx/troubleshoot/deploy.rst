================
Deploying Presto
================

This page presents common problems encountered when deploying Presto. 

.. contents::
    :local:
    :backlinks: none
    :depth: 1

Permission denied for ``launcher.pid``
--------------------------------------

.. rubric:: Problem

Running ``bin/launcher run`` or ``bin/launcher start`` returns the following error:

.. code-block:: none
   :class: no-copy

   Permission denied: '/private/var/presto/data/var/run/launcher.pid'


.. rubric:: Solution

Run the command as root or using ``sudo``:

.. code-block:: shell

   sudo bin/launcher run
   sudo bin/launcher start



Xcode failed to locate Python
-----------------------------

.. rubric:: Problem

.. note::

   macOS only

Running ``bin/launcher run`` or ``bin/launcher start`` returns the following error:

.. code-block:: none
   :class: no-copy

   xcode select failed to locate python requesting installation of command


.. rubric:: Solution

Create a symlink where XCode looks for python that links to python3. An example of such a command: 

.. code-block:: shell

    ln -s /Library/Developer/CommandLineTools/usr/bin/python3 /Library/Developer/CommandLineTools/usr/bin/python



JVM option 'UseG1GC' is experimental
------------------------------------

.. rubric:: Problem

Running ``bin/launcher run`` or ``bin/launcher start`` returns the following error:

.. code-block:: none
   :class: no-copy

    Error: VM option 'UseG1GC' is experimental and must be enabled via -XX:+UnlockExperimentalVM Options.

    Error: Could not create the Java Virtual Machine.

    Error: A fatal exception has occurred. Program will exit.


.. rubric:: Solution

This error occurs with some versions of Java.

Ensure installed Java version meets the `Requirements <https://github.com/prestodb/presto?tab=readme-ov-file#requirements>`_.
