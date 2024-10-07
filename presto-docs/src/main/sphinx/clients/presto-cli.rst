======================
Command Line Interface
======================

.. contents::
    :local:
    :backlinks: none
    :depth: 1

Overview
========

The Presto Command Line Interface (CLI) is a terminal-based interactive shell 
for running queries, and is a
`self-executing <http://skife.org/java/unix/2011/06/20/really_executable_jars.html>`_
JAR file that acts like a normal UNIX executable. The Presto CLI communicates 
with the Presto server over HTTP using a REST API, documented at 
:doc:`Presto Client REST API </develop/client-protocol>`.

To install the Presto CLI, see :doc:`/installation/cli`.

Configuration
=============

The Presto CLI paginates query results using the ``less`` program, which 
is configured with preset options. To change the pagination of query results, set the 
environment variable ``PRESTO_PAGER`` to the name of a different program such as ``more``, 
or set it to an empty value to disable pagination.

Connect to a Presto server using the Presto CLI
===============================================

To connect to a Presto server, run the CLI with the ``--server`` option.  

.. code-block:: none

    ./presto --server localhost:8080 --catalog hive --schema default

``localhost:8080`` is the default for a Presto server, so if you have a Presto server running locally you can 
leave it off. 

To connect to a remote Presto server, use the Presto endpoint URL as in 
the following example command:

.. code-block:: none

    ./presto --server http://www.example.net:8080



Examples
========

Extract a Query Plan in JSON Format
-----------------------------------
The following example runs a SQL statement using ``--execute`` against a 
specified catalog and schema, and saves the query plan of the statement in 
JSON format in a file named ``plan.json``. 

.. code-block:: none

    ./presto --catalog catalogname --schema tpch \
    --execute 'EXPLAIN (format JSON) SELECT 1 from lineitem' \
    --output-format JSON | jq '.["Query Plan"] | fromjson' > plan.json

Online Help
===========

Run the CLI with the ``--help`` option to see the online help.

.. code-block:: none

    ./presto --help

NAME
        presto - Presto interactive console

SYNOPSIS
        presto [--access-token <access token>] [--catalog <catalog>]
                [--client-info <client-info>]
                [--client-request-timeout <client request timeout>]
                [--client-tags <client tags>] [--debug] [--disable-compression]
                [--disable-redirects] [--execute <execute>]
                [--extra-credential <extra-credential>...] [(-f <file> | --file <file>)]
                [(-h | --help)] [--http-proxy <http-proxy>] [--ignore-errors]
                [--insecure] [--keystore-password <keystore password>]
                [--keystore-path <keystore path>] [--keystore-type <keystore type>]
                [--krb5-config-path <krb5 config path>]
                [--krb5-credential-cache-path <krb5 credential cache path>]
                [--krb5-disable-remote-service-hostname-canonicalization]
                [--krb5-keytab-path <krb5 keytab path>]
                [--krb5-principal <krb5 principal>]
                [--krb5-remote-service-name <krb5 remote service name>]
                [--log-levels-file <log levels file>] [--output-format <output-format>]
                [--password] [--resource-estimate <resource-estimate>...]
                [--runtime-stats] [--schema <schema>] [--server <server>]
                [--session <session>...] [--socks-proxy <socks-proxy>]
                [--source <source>] [--truststore-password <truststore password>]
                [--truststore-path <truststore path>]
                [--truststore-type <truststore type>] [--user <user>]
                [--validate-nexturi-source] [--version]

OPTIONS
        --access-token <access token>
            Access token

        --catalog <catalog>
            Default catalog

        --client-info <client-info>
            Extra information about client making query

        --client-request-timeout <client request timeout>
            Client request timeout (default: 2m)

        --client-tags <client tags>
            Client tags

        --debug
            Enable debug information

        --disable-compression
            Disable compression of query results

        --disable-redirects
            Disable client following redirects from server

        --execute <execute>
            Execute specified statements and exit

        --extra-credential <extra-credential>
            Extra credentials (property can be used multiple times; format is
            key=value)

        -f <file>, --file <file>
            Execute statements from file and exit

        -h, --help
            Display help information

        --http-proxy <http-proxy>
            HTTP proxy to use for server connections

        --ignore-errors
            Continue processing in batch mode when an error occurs (default is
            to exit immediately)

        --insecure
            Skip validation of HTTP server certificates (should only be used for
            debugging)

        --keystore-password <keystore password>
            Keystore password

        --keystore-path <keystore path>
            Keystore path

        --keystore-type <keystore type>
            Keystore type

        --krb5-config-path <krb5 config path>
            Kerberos config file path (default: /etc/krb5.conf)

        --krb5-credential-cache-path <krb5 credential cache path>
            Kerberos credential cache path

        --krb5-disable-remote-service-hostname-canonicalization
            Disable service hostname canonicalization using the DNS reverse
            lookup

        --krb5-keytab-path <krb5 keytab path>
            Kerberos key table path (default: /etc/krb5.keytab)

        --krb5-principal <krb5 principal>
            Kerberos principal to be used

        --krb5-remote-service-name <krb5 remote service name>
            Remote peer's kerberos service name

        --log-levels-file <log levels file>
            Configure log levels for debugging using this file

        --output-format <output-format>
            Output format for batch mode [ALIGNED, VERTICAL, JSON, CSV, TSV,
            CSV_HEADER, TSV_HEADER, NULL] (default: CSV)

        --password
            Prompt for password

        --resource-estimate <resource-estimate>
            Resource estimate (property can be used multiple times; format is
            key=value)

        --runtime-stats
            Enable runtime stats information. Flag must be used in conjunction
            with the --debug flag

        --schema <schema>
            Default schema

        --server <server>
            Presto server location (default: localhost:8080)

        --session <session>
            Session property (property can be used multiple times; format is
            key=value; use 'SHOW SESSION' to see available properties)

        --socks-proxy <socks-proxy>
            SOCKS proxy to use for server connections

        --source <source>
            Name of source making query

        --truststore-password <truststore password>
            Truststore password

        --truststore-path <truststore path>
            Truststore path

        --truststore-type <truststore type>
            Truststore type

        --user <user>
            Username

        --validate-nexturi-source
            Validate nextUri server host and port does not change during query
            execution

        --version
            Display version information and exit

