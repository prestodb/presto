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
           "age": "4.45m",
           "lastFailureInfo": {
               "message": "Connect Timeout",
               "stack": [
                   "org.eclipse.jetty.io.ManagedSelector$ConnectTimeout.run(ManagedSelector.java:683)",
                   ....
                   "java.lang.Thread.run(Thread.java:745)"
               ],
               "suppressed": [],
               "type": "java.net.SocketTimeoutException"
           },
           "lastRequestTime": "2017-08-05T11:53:00.647Z",
           "lastResponseTime": "2017-08-05T11:53:00.647Z",
           "recentFailureRatio": 0.47263053472046446,
           "recentFailures": 2.8445543205610617,
           "recentFailuresByType": {
               "java.net.SocketTimeoutException": 2.8445543205610617
           },
           "recentRequests": 6.018558073577414,
           "recentSuccesses": 3.1746446343010297,
           "uri": "http://172.19.0.3:8080"
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
             "age": "1.37m",
             "lastFailureInfo": {
                 "message": "Connect Timeout",
                 "stack": [
                     "org.eclipse.jetty.io.ManagedSelector$ConnectTimeout.run(ManagedSelector.java:683)",
                     .....
                     "java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:617)",
                     "java.lang.Thread.run(Thread.java:745)"
                 ],
                 "suppressed": [],
                 "type": "java.net.SocketTimeoutException"
             },
             "lastRequestTime": "2017-08-05T11:52:42.647Z",
             "lastResponseTime": "2017-08-05T11:52:42.647Z",
             "recentFailureRatio": 0.22498784153043677,
             "recentFailures": 20.11558290058638,
             "recentFailuresByType": {
                 "java.net.SocketTimeoutException": 20.11558290058638
             },
             "recentRequests": 89.40742203558189,
             "recentSuccesses": 69.30583024727453,
             "uri": "http://172.19.0.3:8080"
         }
     ]
