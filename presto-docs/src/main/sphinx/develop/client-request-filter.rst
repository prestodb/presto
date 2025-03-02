=====================
Client Request Filter
=====================

Presto allows operators to customize the headers used to process queries. Some example use cases include customized authentication workflows, or enriching query attributes such as the query source. Use the Client Request Filter plugin to control header customization during query execution.

Implementation
--------------

The ``ClientRequestFilterFactory`` is responsible for creating instances of ``ClientRequestFilter``. It also defines 
the name of the filter.

The ``ClientRequestFilter`` interface provides two methods: ``getExtraHeaders()``, which allows the runtime to quickly check if it needs to apply a more expensive call to enrich the headers, and ``getHeaderNames()``, which returns a list of header names used as the header names in client requests.

The implementation of ``ClientRequestFilterFactory`` must be wrapped as a plugin and installed on the Presto cluster.

After installing a plugin that implements ``ClientRequestFilterFactory`` on the coordinator, the ``AuthenticationFilter`` class passes the ``principal`` object to the request filter, which returns the header values as a map.

Presto uses the request filter to determine whether a header is present in the blocklist. The blocklist includes headers such as ``X-Presto-Transaction-Id``, ``X-Presto-Started-Transaction-Id``, ``X-Presto-Clear-Transaction-Id``, and ``X-Presto-Trace-Token``, which are not allowed to be overridden. 

For a complete list of headers, see the `Java source`_.

Note: The `Java source`_ includes these blocklist headers that are not eligible for overriding. The other headers not mentioned here can be overridden.

.. _Java source: https://github.com/prestodb/presto/blob/master/presto-client/src/main/java/com/facebook/presto/client/PrestoHeaders.java
