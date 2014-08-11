================
Execute Resource
================

.. function:: POST /v1/execute

   :Body: SQL Query to execute
   :Header "X-Presto-User": User to execute statement on behalf of (optional)
   :Header "X-Presto-Source": Source of query
   :Header "X-Presto-Catalog": Catalog to execute query against
   :Header "X-Presto-Schema": Schema to execute query against

   Call this to execute a SQL statement as an alternative to running
   ``/v1/statement``.  Where ``/v1/statement`` will return a
   ``nextUri`` and details about a running query, the ``/v1/execute``
   call will simply execute the SQL statement posted to it and return
   the result set. This service will not return updates about query
   status or details about stages and tasks. It simply executes a
   query and returns the result.

   The sample request and response shown below demonstrate how the
   execute call works. Once you post a SQL statement to /v1/execute it
   returns a set of columns describing an array of data items. This
   trivial executes a "show functions" statement.

   **Example request**:

      .. sourcecode:: http

         POST /v1/execute HTTP/1.1
         Host: localhost:8001
         X-Presto-Schema: jmx
         X-Presto-User: tobrie1
         X-Presto-Catalog: jmx
         Content-Type: text/html
         Content-Length: 14

         show functions

   **Example response**:

      .. sourcecode:: http

         HTTP/1.1 200 OK
	 Content-Type: application/json
	 X-Content-Type-Options: nosniff
	 Transfer-Encoding: chunked

	 {"columns":
	    [
   	       {"name":"Function","type":"varchar"},
	       {"name":"Return Type","type":"varchar"},
	       {"name":"Argument Types","type":"varchar"},
	       {"name":"Function Type","type":"varchar"},
	       {"name":"Description","type":"varchar"}
	    ],
	 "data":
	    [
	       ["abs","bigint","bigint","scalar","absolute value"],
	       ["abs","double","double","scalar","absolute value"],
	       ...
	    ]
	 };
