======================
Command Line Interface
======================

The Presto CLI is a terminal-based interactive shell for running
queries, and is a
`self-executing <http://skife.org/java/unix/2011/06/20/really_executable_jars.html>`_
JAR file that acts like a normal UNIX executable.

Install the Presto CLI
======================

Download :maven_download:`cli`.

Rename the JAR file to ``presto`` with the following command: 

.. code-block:: none

    mv  presto-cli-0.286-executable.jar presto

Use ``chmod +x`` to make the renamed file executable:

.. code-block:: none

    chmod +x presto

Run the Presto CLI
==================

Start the Presto CLI using the name you gave it using the ``mv`` command:

.. code-block:: none

    ./presto

The Presto CLI starts and displays the prompt ``presto>``. 

To exit the Presto CLI, enter ``quit``.

Run the CLI with the ``--help`` option to see the available options.

.. code-block:: none

    ./presto --help

To configure the Presto CLI, or for use and examples, see :doc:`/clients/presto-cli`.
