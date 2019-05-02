=============
Task Resource
=============

The Task resource provides a set of REST endpoints that give Presto
servers the ability to communicate about tasks and task output. This
isn't a service that will be used by end users, but it supports the
execution of queries on a Presto installation.

.. function:: GET /v1/task

   Returns information about all tasks known to a Presto server.

   Note that the output of a call to ``/v1/task`` can be quite
   large. If you execute this against a busy Presto server the
   response received will include a listing of every task known to
   that server along with detailed statistics about operators and
   drivers.

   The following example response shows a trivial task response that
   has been truncated to fit this manual. A real response from a busy
   Presto server would generate pages and pages of output.  Here there
   is a ``taskId`` for a task which is in the ``CANCELED`` state.

   **Example response**:

   .. sourcecode:: http

      [ {
        "taskId" : "20131222_183944_00011_dk5x2.1.0",
        "version" : 9223372036854775807,
        "state" : "CANCELED",
        "self" : "unknown",
        "lastHeartbeat" : "2013-12-22T13:54:46.566-05:00",
        "outputBuffers" : {
          "state" : "FINISHED",
          "masterSequenceId" : 0,
          "pagesAdded" : 0,
          "buffers" : [ ]
        },
        "noMoreSplits" : [ ],
        "stats" : {
          "createTime" : "2013-12-22T13:54:46.566-05:00",
          "elapsedTime" : "0.00ns",
          "queuedTime" : "92.00us",
          "totalDrivers" : 0,
          "queuedDrivers" : 0,
          "runningDrivers" : 0,
          "completedDrivers" : 0,
          "memoryReservation" : "0B",
          "totalScheduledTime" : "0.00ns",
          "totalCpuTime" : "0.00ns",
          "totalBlockedTime" : "0.00ns",
          "rawInputDataSize" : "0B",
          "rawInputPositions" : 0,
          "processedInputDataSize" : "0B",
          "processedInputPositions" : 0,
          "outputDataSize" : "0B",
          "outputPositions" : 0,
          "pipelines" : [ ]
        },
        "failures" : [ ],
        "outputs" : { }
      }]

.. function:: POST /v1/task/{taskId}

.. function:: DELETE /v1/task/{taskId}

   Deletes a given task from a Presto server.

.. function:: GET /v1/task/{taskId}

   Retrieves information about a specific task by ``taskId``.

   The following example lists the output of a task.  It contains the
   following high-level sections:

   * ``outputBuffers``
   * ``noMoreSplits``
   * ``stats``
   * ``failures``
   * ``outputs``

   This is the same output that is also present in the response from
   the Query resource which lists all of the stages and tasks involved
   in a particular query. This is call is used by Presto to coordinate
   a queries.

   **Example response**:

   .. sourcecode:: http

      {
	"taskId" : "20140115_170528_00004_dk5x2.0.0",
	"version" : 42,
	"state" : "FINISHED",
	"self" : "http://10.193.207.128:8080/v1/task/20140115_170528_00004_dk5x2.0.0",
	"lastHeartbeat" : "2014-01-15T12:12:12.518-05:00",
	"outputBuffers" : {
	  "state" : "FINISHED",
	  "masterSequenceId" : 0,
	  "pagesAdded" : 1,
	  "buffers" : [ {
	    "bufferId" : "out",
	    "finished" : true,
	    "bufferedPages" : 0,
	    "pagesSent" : 1
	  } ]
	},
	"noMoreSplits" : [ "8" ],
	"stats" : {
	  "createTime" : "2014-01-15T12:12:08.520-05:00",
	  "startTime" : "2014-01-15T12:12:08.526-05:00",
	  "endTime" : "2014-01-15T12:12:12.518-05:00",
	  "elapsedTime" : "4.00s",
	  "queuedTime" : "6.39ms",
	  "totalDrivers" : 1,
	  "queuedDrivers" : 0,
	  "runningDrivers" : 0,
	  "completedDrivers" : 1,
	  "memoryReservation" : "174.76kB",
	  "totalScheduledTime" : "4.19ms",
	  "totalCpuTime" : "4.09ms",
	  "totalBlockedTime" : "29.50ms",
	  "rawInputDataSize" : "10.90kB",
	  "rawInputPositions" : 154,
	  "processedInputDataSize" : "10.90kB",
	  "processedInputPositions" : 154,
	  "outputDataSize" : "10.90kB",
	  "outputPositions" : 154,
	  "pipelines" : [ {
	    "inputPipeline" : true,
	    "outputPipeline" : true,
	    "totalDrivers" : 1,
	    "queuedDrivers" : 0,
	    "runningDrivers" : 0,
	    "completedDrivers" : 1,
	    "memoryReservation" : "0B",
	    "queuedTime" : {
	      "maxError" : 0.0,
	      "count" : 1.0,
	      "total" : 5857000.0,
	      "p01" : 5857000,
	      "p05" : 5857000,
	      "p10" : 5857000,
	      "p25" : 5857000,
	      "p50" : 5857000,
	      "p75" : 5857000,
	      "p90" : 5857000,
	      "p95" : 5857000,
	      "p99" : 5857000,
	      "min" : 5857000,
	      "max" : 5857000
	    },
	    "elapsedTime" : {
	      "maxError" : 0.0,
	      "count" : 1.0,
	      "total" : 4.1812E7,
	      "p01" : 41812000,
	      "p05" : 41812000,
	      "p10" : 41812000,
	      "p25" : 41812000,
	      "p50" : 41812000,
	      "p75" : 41812000,
	      "p90" : 41812000,
	      "p95" : 41812000,
	      "p99" : 41812000,
	      "min" : 41812000,
	      "max" : 41812000
	    },
	    "totalScheduledTime" : "4.19ms",
	    "totalCpuTime" : "4.09ms",
	    "totalBlockedTime" : "29.50ms",
	    "rawInputDataSize" : "10.90kB",
	    "rawInputPositions" : 154,
	    "processedInputDataSize" : "10.90kB",
	    "processedInputPositions" : 154,
	    "outputDataSize" : "10.90kB",
	    "outputPositions" : 154,
	    "operatorSummaries" : [ {
	      "operatorId" : 0,
	      "operatorType" : "ExchangeOperator",
	      "addInputCalls" : 0,
	      "addInputWall" : "0.00ns",
	      "addInputCpu" : "0.00ns",
	      "addInputUser" : "0.00ns",
	      "inputDataSize" : "10.90kB",
	      "inputPositions" : 154,
	      "getOutputCalls" : 1,
	      "getOutputWall" : "146.00us",
	      "getOutputCpu" : "137.90us",
	      "getOutputUser" : "0.00ns",
	      "outputDataSize" : "10.90kB",
	      "outputPositions" : 154,
	      "blockedWall" : "29.50ms",
	      "finishCalls" : 0,
	      "finishWall" : "0.00ns",
	      "finishCpu" : "0.00ns",
	      "finishUser" : "0.00ns",
	      "memoryReservation" : "0B",
	      "info" : {
		"bufferedBytes" : 0,
		"averageBytesPerRequest" : 11158,
		"bufferedPages" : 0,
		"pageBufferClientStatuses" : [ {
		  "uri" : "http://10.193.207.128:8080/v1/task/20140115_170528_00004_dk5x2.1.0/results/ab68e201-3878-4b21-b6b9-f6658ddc408b",
		  "state" : "closed",
		  "lastUpdate" : "2014-01-15T12:12:08.562-05:00",
		  "pagesReceived" : 1,
		  "requestsScheduled" : 3,
		  "requestsCompleted" : 3,
		  "httpRequestState" : "queued"
		} ]
	      }
	    }, {
	      "operatorId" : 1,
	      "operatorType" : "FilterAndProjectOperator",
	      "addInputCalls" : 1,
	      "addInputWall" : "919.00us",
	      "addInputCpu" : "919.38us",
	      "addInputUser" : "0.00ns",
	      "inputDataSize" : "10.90kB",
	      "inputPositions" : 154,
	      "getOutputCalls" : 2,
	      "getOutputWall" : "128.00us",
	      "getOutputCpu" : "128.64us",
	      "getOutputUser" : "0.00ns",
	      "outputDataSize" : "10.45kB",
	      "outputPositions" : 154,
	      "blockedWall" : "0.00ns",
	      "finishCalls" : 5,
	      "finishWall" : "258.00us",
	      "finishCpu" : "253.19us",
	      "finishUser" : "0.00ns",
	      "memoryReservation" : "0B"
	    }, {
	      "operatorId" : 2,
	      "operatorType" : "OrderByOperator",
	      "addInputCalls" : 1,
	      "addInputWall" : "438.00us",
	      "addInputCpu" : "439.18us",
	      "addInputUser" : "0.00ns",
	      "inputDataSize" : "10.45kB",
	      "inputPositions" : 154,
	      "getOutputCalls" : 4,
	      "getOutputWall" : "869.00us",
	      "getOutputCpu" : "831.85us",
	      "getOutputUser" : "0.00ns",
	      "outputDataSize" : "10.45kB",
	      "outputPositions" : 154,
	      "blockedWall" : "0.00ns",
	      "finishCalls" : 4,
	      "finishWall" : "808.00us",
	      "finishCpu" : "810.18us",
	      "finishUser" : "0.00ns",
	      "memoryReservation" : "174.76kB"
	    }, {
	      "operatorId" : 3,
	      "operatorType" : "FilterAndProjectOperator",
	      "addInputCalls" : 1,
	      "addInputWall" : "166.00us",
	      "addInputCpu" : "166.66us",
	      "addInputUser" : "0.00ns",
	      "inputDataSize" : "10.45kB",
	      "inputPositions" : 154,
	      "getOutputCalls" : 5,
	      "getOutputWall" : "305.00us",
	      "getOutputCpu" : "241.14us",
	      "getOutputUser" : "0.00ns",
	      "outputDataSize" : "10.90kB",
	      "outputPositions" : 154,
	      "blockedWall" : "0.00ns",
	      "finishCalls" : 2,
	      "finishWall" : "70.00us",
	      "finishCpu" : "71.02us",
	      "finishUser" : "0.00ns",
	      "memoryReservation" : "0B"
	    }, {
	      "operatorId" : 4,
	      "operatorType" : "TaskOutputOperator",
	      "addInputCalls" : 1,
	      "addInputWall" : "50.00us",
	      "addInputCpu" : "51.03us",
	      "addInputUser" : "0.00ns",
	      "inputDataSize" : "10.90kB",
	      "inputPositions" : 154,
	      "getOutputCalls" : 0,
	      "getOutputWall" : "0.00ns",
	      "getOutputCpu" : "0.00ns",
	      "getOutputUser" : "0.00ns",
	      "outputDataSize" : "10.90kB",
	      "outputPositions" : 154,
	      "blockedWall" : "0.00ns",
	      "finishCalls" : 1,
	      "finishWall" : "35.00us",
	      "finishCpu" : "35.39us",
	      "finishUser" : "0.00ns",
	      "memoryReservation" : "0B"
	    } ],
	    "drivers" : [ ]
	  } ]
	},
	"failures" : [ ],
	"outputs" : { }
      }

.. function:: GET /v1/task/{taskId}/results/{outputId}/{token}

   This service is used by Presto to retrieve task output.

.. function:: DELETE /v1/task/{taskId}/results/{outputId}

   This service is used by Presto to delete task output.
