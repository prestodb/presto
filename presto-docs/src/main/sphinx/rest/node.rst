=============
Node Resource
=============

.. function:: GET /v1/node

   Returns a list of nodes known to a Presto Server. This call
   doesn't require a query parameter of headers, it simply returns an
   array with each known node in a Presto installation.

   In the response, the values ``recentRequests``, ``recentFailures``,
   and ``recentSuccesses`` are decaying counters set to decay
   exponentially over time with a decay parameter of one minute. This
   decay rate means that if a Presto node has 1000 successes in a few
   seconds, this statistic value will drop to 367 in one minute.

   ``age`` shows you how long a particular node has been running, and uri
   points you to the HTTP server for a particular node. The last
   request and last response times show you how recently a node has
   been used.

   The following example response displays a single node which has not
   experienced any failure conditions. Each node also reports
   statistics about traffic uptime, and failures.

   **Example response**:

      .. sourcecode:: http

         HTTP/1.1 200 OK
         Vary: Accept
         Content-Type: text/javascript

         [
	   {
       	     "uri":"http://10.209.57.156:8080",
	     "recentRequests":25.181940555111073,
	     "recentFailures":0.0,
	     "recentSuccesses":25.195472984170983,
	     "lastRequestTime":"2013-12-22T13:32:44.673-05:00",
	     "lastResponseTime":"2013-12-22T13:32:44.677-05:00",
	     "age":"14155.28ms",
	     "recentFailureRatio":0.0,
	     "recentFailuresByType":{}
	   }
	 ]


   If a node is experiencing errors, you'll see a response that looks
   like the following. Here we have a node which has experienced a
   spate of errors. The recentFailuresByType field lists the Java
   exception which have occurred recently on a particular node.

   **Example response with Errors**:

      .. sourcecode:: http


         HTTP/1.1 200 OK
	 Vry: Accept
	 Content-Type: text/javascript

	 [
	   {
	     "uri":"http://10.209.57.156:8080",
	     "recentRequests":117.0358348572745,
	     "recentFailures":8.452831267323281,
	     "recentSuccesses":108.58300358995123,
	     "lastRequestTime":"2013-12-23T02:00:40.382-05:00",
	     "lastResponseTime":"2013-12-23T02:00:40.383-05:00",
	     "age":"44882391.57ms",
	     "recentFailureRatio":0.07222430016952953,
	     "recentFailuresByType":
	     {
	       "java.io.IOException":0.9048374180359595,
	       "java.net.SocketTimeoutException":6.021822867514955E-269,
	       "java.net.ConnectException":7.54799384928732
	     }
	   }
	 ]

.. function:: GET /v1/node/failed

   Calling this service returns a JSON document listing all the nodes
   that have failed the last heartbeat check. The information
   returned by this call is the same as the information returned by
   the previous service.

   **Example response**:

      .. sourcecode:: http

         [
	    {
	       "uri":"http://10.209.57.156:8080",
	       "recentRequests":5.826871111529161,
	       "recentFailures":0.4208416882082422,
	       "recentSuccesses":5.406029423320919,
	       "lastRequestTime":"2013-12-23T02:00:40.382-05:00",
	       "lastResponseTime":"2013-12-23T02:00:40.383-05:00",
	       "age":"45063192.35ms",
	       "recentFailureRatio":0.07222430016952952,
	       "recentFailuresByType":
	       {
	          "java.io.IOException":0.0450492023935578,
		  "java.net.SocketTimeoutException":2.998089068041336E-270,
		  "java.net.ConnectException":0.3757924858146843
	       }
	    }
	 ]
