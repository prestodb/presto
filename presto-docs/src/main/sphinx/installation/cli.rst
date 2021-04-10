======================
Command Line Interface
======================

The Presto CLI provides a terminal-based interactive shell for running
queries. The CLI is a
`self-executing <http://skife.org/java/unix/2011/06/20/really_executable_jars.html>`_
JAR file, which means it acts like a normal UNIX executable.

Download :maven_download:`cli`, rename it to ``presto``,
make it executable with ``chmod +x``, then run it:

.. code-block:: none

    ./presto --server localhost:8080 --catalog hive --schema default

Run the CLI with the ``--help`` option to see the available options.

By default, the results of queries are paginated using the ``less`` program
which is configured with a carefully selected set of options. This behavior
can be overridden by setting the environment variable ``PRESTO_PAGER`` to the
name of a different program such as ``more``, or set it to an empty value
to completely disable pagination.

Documentation on the HTTP protocol between the Presto CLI and the Presto
engine can be found :doc:`here </develop/client-protocol>`.
