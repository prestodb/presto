======================
Command Line Interface
======================

The Presto CLI is a terminal-based interactive shell for running
queries. The CLI is a
`self-executing <http://skife.org/java/unix/2011/06/20/really_executable_jars.html>`_
JAR file that acts like a normal UNIX executable.

Download :maven_download:`cli`

Rename the JAR file to ``presto`` with the following command 

    mv  :maven_download:`cli` presto

Use ``chmod +x`` to make the file executable

    chmod +x presto

 then start the Presto CLI

    ./presto

The prompt ``presto>`` is displayed. 

To exit the Presto CLI, enter ``quit``.

Run the CLI with the ``--help`` option to see the available options.

    ./presto --help

To connect to a Presto server, run the CLI with the --server option.  

    ./presto --server localhost:8080 --catalog hive --schema default

``localhost:8080`` is the default for a Presto server, so if you have a Presto server running locally you can 
leave it off. If you are connecting to a remote Presto server, use the Presto endpoint URL as in 
the following example command

   ./presto --server http://www.example.net:8080

The Presto CLI paginates query results using the ``less`` program, which 
is configured with preset options. To change the pagination of query results, set the 
environment variable ``PRESTO_PAGER`` to the name of a different program such as ``more``, 
or set it to an empty value to disable pagination.

The Presto engine and client communicate over HTTP using a REST API. The documentation for the API is at 
:doc:`Presto Client REST API </develop/client-protocol>`.
