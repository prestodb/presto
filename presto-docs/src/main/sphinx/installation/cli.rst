======================
Command Line Interface
======================

The Presto CLI is a terminal-based interactive shell for running
queries. The CLI is a
`self-executing <http://skife.org/java/unix/2011/06/20/really_executable_jars.html>`_
JAR file that acts like a normal UNIX executable.

Download :maven_download:`cli`

Rename it to ``presto`` with the following command 

    mv  :maven_download:`cli` presto

Make it executable with ``chmod +x``

    chmod +x presto

 then run it:

    ./presto
The prompt ``presto>`` is displayed. To exit the Presto CLI, enter ``quit``.

Run the CLI with the ``--help`` option to see the available options.

    ./presto --help

To connect to a Presto server, run the CLI with the --server option 
The Presto CLI displays query results as paginated using the ``less`` program, which 
is configured with preset options. To change the pagination of query results, set the 
environment variable ``PRESTO_PAGER`` to the name of a different program such as ``more``, 
or set it to an empty value to disable pagination.

Documentation on the HTTP protocol between the Presto CLI and the Presto
engine can be found :doc:`here </develop/client-protocol>`.
