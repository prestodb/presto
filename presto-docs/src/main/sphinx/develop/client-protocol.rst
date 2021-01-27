======================
Presto Client REST API
======================

The Presto client allows users to submit Presto queries and view results.  This note documents the REST API
used by the Presto client.

HTTP Methods
============

* A ``POST`` to ``/v1/statement`` runs the query string in the ``POST`` body, and returns a JSON document containing
  the query results.  If there are more results, the JSON document will contain a ``nextUri``
  URL attribute.
* A ``GET`` to the ``nextUri`` attribute returns the next batch of query results.
* A ``DELETE`` to ``nextUri`` terminates a running query.

Overview of Query Processing
============================

A Presto client request is initiated by an HTTP ``POST`` to the endpoint ``/v1/statement``, with a ``POST`` body
consisting of the SQL query string.  The caller may set header ``X-Presto-User`` to the username for the session,
as well as a long list of other headers, documented below.

If the client request returns an HTTP 503, that means the server was busy, and the client should try again
in 50-100 milliseconds.  Any HTTP status other than 503 or 200 means that the query has failed.

The ``/v1/statement`` ``POST`` request returns a JSON document of type ``QueryResults``, as well as a collection
of response headers.  The ``QueryResults`` document will contain an ``error`` field of type ``QueryError``
if the query has failed, and if that object is not present, the query succeeded.  Important members of
``QueryResults`` are documented below.

If the ``data`` field of the JSON document is set, it contains a list of the rows
of data, and the ``columns`` field will also be set to a list of the names and types of
the columns returned by the query.  Most of the response headers should be treated like
browser cookies by the client, and echoed back as request headers in subsequent client requests,
as documented below.

If the JSON document returned by the ``POST`` to ``/v1/statement`` does not contain a ``nextUri`` link, the query has completed,
either successfully or unsuccessfully, and no additional requests need to be made.  If the ``nextUri`` link is present in
the document, there are more query results to be fetched.  The client should loop executing a ``GET`` request
to the ``nextUri`` returned in the ``QueryResults`` response object until ``nextUri`` is absent from the response.

The ``status`` field of the JSON document is for human consumption only, and provides a hint about
the query's state on the server.  It is not synchronized with the server's query state and should not
be used to tell if the query is finished.

Important ``QueryResults`` Attributes
=====================================

The most important attributes of the ``QueryResults`` JSON document returned by the REST API
endpoints are listed in this table.  Refer to the class ``QueryResults`` for more details.

====================================== ===========================================================================================================================
Attribute                              Description
====================================== ===========================================================================================================================
``id``                                 The ID of the query.
``nextUri``                            If present, the URL that should be used in subsequent ``GET`` or ``DELETE`` requests.  If not present, the query is
                                       complete or ended in error.
``columns``                            A list of the names and types of the columns returned by the query.
``data``                               The ``data`` attribute contains a list of the rows returned by the query request.  Each row is itself a
                                       list that holds values of the columns in the row, in the order specified by the ``columns`` attribute.
``updateType``                         A human-readable string representing the operation.  For a ``CREATE TABLE`` request, the ``updateType`` will be,
                                       "CREATE TABLE"; for ``SET SESSION`` it will be "SET SESSION"; etc.
``error``                              If query failed, the ``error`` attribute will contain JSON for a ``QueryError`` object.  That object contains
                                       a ``message``, an ``errorCode`` and other information about the error.  See the ``QueryError`` class for more details.
====================================== ===========================================================================================================================


Client Request Headers
======================

This table lists all supported client request headers.  Many of the headers can are updated in the client
by response headers, and supplied in subsequent requests, just like browser cookies.

====================================== =========================================================================================
Request Header Name                    Description
====================================== =========================================================================================
``X-Presto-User``                      Specifies the session user; must be supplied with every
                                       request to ``/v1/statement``.
``X-Presto-Source``                    For reporting purposes, this supplies the name of the software that submitted the query.
``X-Presto-Catalog``                   The catalog to be used when running the query.  Set by response header
                                       ``X-Presto-Set-Catalog``.
``X-Presto-Schema``                    The schema to be used when running the query.  Set by response header
                                       ``X-Presto-Set-Schema``.
``X-Presto-Time-Zone``                 The timezone to be used when running the query, which by default is the timezone of
                                       the Presto engine.
``X-Presto-Language``                  The language to be used when running the query and formatting results.  The language
                                       of the session can be set on a per-query basis using the ``X-Presto-Language``
                                       HTTP header, or via the ``PrestoConnection.setLocale(Locale)`` method in the
                                       JDBC driver.
``X-Presto-Trace-Token``               Supplies a trace token to the Presto engine to help identify log lines that originate
                                       with this query request.
``X-Presto-Session``                   Supplies a comma-separated list of name=value pairs as session properties.
                                       When the Presto client run a ``SET SESSION name=value`` query, the name=value pair
                                       is returned in the ``X-Set-Presto-Session`` response header, and added to the client's
                                       list of session properties.
                                       If the response header ``X-Presto-Clear-Session`` is returned, its value
                                       is the name of a session property that will be removed from the client's accumulated
                                       list.
``X-Presto-Role``                      Sets the catalog role that will be used in this request.  Set by response header
                                       ``X-Presto-Set-Role``.
``X-Presto-Prepared-Statement``        A comma-separated list of the name=value pairs, where the names are names of
                                       previously prepared SQL statements, and the values are keys that identify the executable
                                       form of the named prepared statements.
``X-Presto-Transaction-Id``            The transaction ID to be used when running the query.  Set by response header
                                       ``X-Presto-Started-Transaction-Id`` and cleared by ``X-Presto-Clear-Transaction-Id``.
``X-Presto-Client-Info``               Contains arbitrary information about the client program submitting the query.
``X-Presto-Client-Tags``               A comma-separated list of "tag" strings, used to identify Presto resource groups.
``X-Presto-Resource-Estimate``         A comma-separated list of ``resource=value`` type assigments.  The possible choices
                                       of ``resource`` are "EXECUTION_TIME", "CPU_TIME",  "PEAK_MEMORY" and "PEAK_TASK_MEMORY".
                                       "EXECUTION_TIME" and "CPU_TIME" have values specified as airlift ``Duration`` strings,
                                       whose format is a double precision number followed by a ``TimeUnit`` string, e.g.,
                                       of ``s`` for seconds, ``m`` for minutes, ``h`` for hours, etc.  "PEAK_MEMORY" and
                                       "PEAK_TASK_MEMORY" are specified as as airlift ``DataSize`` strings, whose format
                                       is an integer followed by ``B`` for bytes; ``kB`` for kilobytes; ``mB`` for megabytes,
                                       ``gB`` for gigabytes, etc.
``X-Presto-Extra-Credential``          Provides extra credentials to the connector.  The header is a name=value string that
                                       is saved in the session ``Identity`` object.  The name and value are only
                                       meaningful to the connector.
====================================== =========================================================================================


Client Response Headers
=======================

This table lists the supported client response headers.  After receiving a response, a client must update the
request headers that will be used in subsequent requests to be consistent with the response headers received.

====================================== =================================================================================================
Respone Header Name                    Description
====================================== =================================================================================================
``X-Presto-Set-Catalog``               Instructs the client to set the catalog that will be sent in the
                                       ``X-Presto-Catalog`` request header in subsequent client requests.
``X-Presto-Set-Schema``                Instructs the client to set the schema that will be sent in the ``X-Presto-Schema`` request
                                       header in subsequent client requests.
``X-Presto-Set-Session``               The value of the ``X-Presto-Set-Session`` response header is a name=value string,
                                       representing session attributes that are meaningful to the Presto engine or a connector.
                                       Instructs the client to add that name=value string to the ``X-Presto-Session``
                                       request header to be used in subsequent client requests.
``X-Presto-Clear-Session``             Instructs the client to remove the session property with the whose name is the value
                                       of the ``X-Presto-Clear-Session`` header from the comma-separated list of session properties
                                       that will be sent in the ``X-Presto-Session`` header in subsequent client requests.
``X-Presto-Set-Role``                  Instructs the client to set ``X-Presto-Role`` request header to the catalog role give by the
                                       value of the ``X-Presto-Set-Role`` headerin subsequent client requests.
``X-Presto-Added-Prepare``             Instructs the client to add the name=value pair to the set of prepared statements
                                       that will be sent in the ``X-Presto-Prepared-Statements`` request header
                                       in subsequent client requests.
``X-Presto-Deallocated-Prepare``       Instructs the client to remove the prepared statement whose name is the value of the
                                       ``X-Presto-Deallocated-Prepare`` header from the client's list of prepared statements
                                       sent in the ``X-Presto-Prepared-Statements`` request header in subsequent client requests.
``X-Presto-Started-Transaction-Id``    Provides the transaction ID that the client should pass back in the
                                       ``X-Presto-Transaction-Id`` request header in subsequent requests.
``X-Presto-Clear-Transaction-Id``      Instructs the client to clear the ``X-Presto-Transaction-Id`` request header used in
                                       subsequent requests.
====================================== =================================================================================================


``QueryResults``
================

When a query is executed by the client, a ``QueryResults`` object is returned.  ``QueryResults`` contains
a long list of data members.  These data members may be useful in tracking down problems:

========================== ============================ ================================================================================
Data Member                Type                         Notes
========================== ============================ ================================================================================
``queryError``             ``QueryError``               Non-null only if the query resulted in an error.  ``QueryResults.failureInfo``
                                                        of type ``FailureInfo`` has detail on the reason for the failure, including a
                                                        stack trace, and ``FailureInfo.errorLocation``, providing the query line
                                                        number and column number where the failure was detected.
``warnings``               ``List<PrestoWarning>``      A usually-empty list of warnings.
``statementStats``         ``StatementStats``           A class containing statistics about the query execution.  Of particular
                                                        interest is ``StatementStats.rootStage``, of type ``StageStats``, providing
                                                        statistics on the execution of each of the stages of query processing.
========================== ============================ ================================================================================


``PrestoHeaders``
=================

Class ``PrestoHeaders`` enumerates all the HTTP request and response headers allowed by the Presto client REST API.
