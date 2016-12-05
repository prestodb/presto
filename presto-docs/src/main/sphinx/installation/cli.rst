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

The available options for the CLI are:

.. code-block:: none

        --catalog <catalog>
            Default catalog to connect to

        --client-request-timeout <client request timeout>
            Client request timeout (default: 2m)

        --debug
            Enable debug information

        --enable-authentication
            Enable client authentication

        --execute <execute>
            Execute specified statements and exit

        -f <file>, --file <file>
            Execute statements from file and exit

        -h, --help
            Display help information

        --keystore-password <keystore password>
            The password for the keystore. This must match the password
            specified when creating the keystore

        --keystore-path <keystore path>
            The location of the Java Keystore file that will be used to
            secure TLS

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
            Output format for batch mode [ALIGNED, VERTICAL, CSV, TSV,
            CSV_HEADER, TSV_HEADER, NULL] (default: CSV)

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
            Name of source invoking the query (default: presto-cli)

        --truststore-password <truststore password>
            The password for the truststore. This must match the password you
            specified when creating the truststore

        --truststore-path <truststore path>
            The location of the Java Truststore file that will be used to
            secure TLS

        --user <user>
            Username

        --version
            Version of the CLI

By default, the results of queries are paginated using the ``less`` program
which is configured with a carefully selected set of options. This behavior
can be overridden by setting the environment variable ``PRESTO_PAGER`` to the
name of a different program such as ``more``, or set it to an empty value
to completely disable pagination.
