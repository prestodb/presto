==============
Query Resource
==============

The Query REST service is the most complex of the rest services. It
contains detailed information about nodes, and other
details that capture the state and history of a query being executed
on a Presto installation.

.. function:: GET /v1/query

   This service returns information and statistics about queries that
   are currently being executed on a Presto coordinator.

   When you point a web broswer at a Presto coordinate you'll see a
   rendered version of the output from this service which will display
   recent queries that have executed on a Presto installation.

.. function:: GET /v1/query/{queryId}

   If you are looking to gather very detailed statistics about a
   query, this is the service you would call. If you load the web
   interface of a Presto coordinator you will see a list of current
   queries. Clicking on a query will reveal a link to this service.

   **Example response**:

      .. sourcecode:: http

         {
  	    "queryId" : "20131229_211533_00017_dk5x2",
  	    "session" : {
    	       "user" : "tobrien",
    	       "source" : "presto-cli",
    	       "catalog" : "jmx",
    	       "schema" : "jmx",
    	       "remoteUserAddress" : "173.15.79.89",
    	       "userAgent" : "StatementClient/0.55-SNAPSHOT",
    	       "startTime" : 1388351852026
  	    },
  	    "state" : "FINISHED",
  	    "self" : "http://10.193.207.128:8080/v1/query/20131229_211533_00017_dk5x2",
  	    "fieldNames" : [ "name" ],
  	    "query" : "select name from \"java.lang:type=runtime\"",
  	    "queryStats" : {
    	       "createTime" : "2013-12-29T16:17:32.027-05:00",
    	       "executionStartTime" : "2013-12-29T16:17:32.086-05:00",
    	       "lastHeartbeat" : "2013-12-29T16:17:44.561-05:00",
    	       "endTime" : "2013-12-29T16:17:32.152-05:00",
    	       "elapsedTime" : "125.00ms",
    	       "queuedTime" : "1.31ms",
    	       "analysisTime" : "4.84ms",
    	       "totalTasks" : 2,
    	       "runningTasks" : 0,
    	       "completedTasks" : 2,
    	       "totalDrivers" : 2,
    	       "queuedDrivers" : 0,
    	       "runningDrivers" : 0,
    	       "completedDrivers" : 2,
    	       "totalMemoryReservation" : "0B",
    	       "totalScheduledTime" : "5.84ms",
    	       "totalCpuTime" : "710.49us",
    	       "totalBlockedTime" : "27.38ms",
    	       "rawInputDataSize" : "27B",
    	       "rawInputPositions" : 1,
    	       "processedInputDataSize" : "32B",
    	       "processedInputPositions" : 1,
    	       "outputDataSize" : "32B",
    	       "outputPositions" : 1
  	    },
  	    "outputStage" : ...
         }

