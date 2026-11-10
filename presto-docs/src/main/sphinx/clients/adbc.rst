===========
ADBC Driver
===========

The ADBC driver for Presto lets applications that use
`Arrow Database Connectivity (ADBC) <https://arrow.apache.org/adbc/>`_ query
Presto and receive results as Apache Arrow record batches. Any language with
an ADBC driver manager can use it, including Python, R, Go, C/C++, Rust, and
Java, and Arrow-native tools such as pandas, Polars, and DuckDB can consume the
results directly.

The driver is maintained in the
`adbc-drivers/presto <https://github.com/adbc-drivers/presto>`_ GitHub
repository. It is a client-side component that uses the Presto REST protocol,
so it requires no changes to the Presto server. See the
`driver documentation <https://adbc-drivers.org/drivers/presto/>`_ for the
full feature and type support tables.

Installation
------------

Install the driver with `dbc <https://docs.columnar.tech/dbc>`_:

.. code-block:: none

    dbc install presto

Then install the ADBC driver manager for your language. For Python:

.. code-block:: none

    pip install adbc-driver-manager pyarrow

Connection URI
--------------

Pass a connection URI as the ``uri`` option:

.. code-block:: none

    presto://[user[:password]@]host[:port][/catalog[/schema]][?param=value&...]

The driver also accepts ``http://`` and ``https://`` URIs. Use ``https://``,
or one of the TLS parameters below, to connect to a coordinator over TLS.

=====================  ========================================================
Parameter              Description
=====================  ========================================================
``ssl_ca``             Path to a PEM CA certificate used to verify the server.
``ssl_cert``           Path to a PEM client certificate for mutual TLS.
``ssl_key``            Path to the PEM private key for ``ssl_cert``.
``ssl_skip_verify``    Set to ``true`` to skip server certificate verification.
                       Use only for development with self-signed certificates.
``source``             Value of the ``X-Presto-Source`` header.
``client_tags``        Comma-separated client tags.
``client_info``        Value of the ``X-Presto-Client-Info`` header.
``timezone``           Session time zone.
=====================  ========================================================

Any other query parameter is sent to Presto as a session property. Catalog
session properties use their dotted name. Presto rejects unknown session
property names when the first query runs:

.. code-block:: none

    https://analyst@presto.example.com:8443/hive/default?query_max_run_time=10m&hive.max_split_size=64MB

Reserved characters in the user name, password, or other URI elements must be
percent-encoded. For example, ``@`` becomes ``%40``.

Python Example
--------------

.. code-block:: python

    from adbc_driver_manager import dbapi

    with dbapi.connect(
        driver="presto",
        db_kwargs={"uri": "https://analyst@presto.example.com:8443/tpch/tiny"},
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT nationkey, name FROM nation WHERE regionkey = ?", parameters=(1,))
            table = cur.fetch_arrow_table()

    print(table.to_pandas())

Polars can read from the same connection with
``polars.read_database(query, connection=conn)``.

Go Example
----------

.. code-block:: go

    import (
        "context"

        "github.com/apache/arrow-adbc/go/adbc/drivermgr"
    )

    func run(ctx context.Context) error {
        var drv drivermgr.Driver
        db, err := drv.NewDatabase(map[string]string{
            "driver": "presto",
            "uri":    "https://analyst@presto.example.com:8443/tpch/tiny",
        })
        if err != nil {
            return err
        }
        defer db.Close()

        conn, err := db.Open(ctx)
        if err != nil {
            return err
        }
        defer conn.Close()

        stmt, err := conn.NewStatement()
        if err != nil {
            return err
        }
        defer stmt.Close()

        if err := stmt.SetSqlQuery("SELECT nationkey, name FROM nation"); err != nil {
            return err
        }
        reader, _, err := stmt.ExecuteQuery(ctx)
        if err != nil {
            return err
        }
        defer reader.Release()

        for reader.Next() {
            batch := reader.RecordBatch()
            _ = batch // process the Arrow record batch
        }
        return reader.Err()
    }

Type Mapping
------------

Presto returns query results as JSON over the REST protocol, so some types are
returned as text instead of their natural Arrow type:

* ``DECIMAL`` values are returned as decimal strings.
* ``ARRAY``, ``MAP``, and ``ROW`` values are returned as JSON text.
* ``JSON``, ``IPADDRESS``, ``HyperLogLog``, geospatial, and other types without
  an Arrow equivalent are returned as their text representation.

Every Arrow field carries the original Presto type name in its field metadata,
so applications can recover the source type. All other types, including
integers, floating point, ``BOOLEAN``, ``VARCHAR``, ``VARBINARY``, ``DATE``,
``TIME``, ``TIMESTAMP``, intervals, and ``UUID``, map to the corresponding
Arrow types.

Supported Features
------------------

* Query execution with Arrow results, and query cancellation
* Positional parameter binding and client-side prepared statements
* Catalog, schema, table, and column metadata through ``GetObjects`` and
  ``GetTableSchema``
* Table statistics through ``GetStatistics``, backed by ``SHOW STATS``
* Bulk ingestion in create, append, replace, and create-append modes
* TLS with a custom CA, mutual TLS, and HTTP basic authentication

Transactions, partitioned result sets (``ExecutePartitions``), and Substrait
plans are not supported. Connections always run in autocommit mode. Whether a connector supports bulk ingestion depends on whether it
supports ``CREATE TABLE`` and ``INSERT``.
