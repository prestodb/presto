=======
Queries
=======

This page presents problems encountered with queries in Presto. 

.. contents::
    :local:
    :backlinks: none
    :depth: 1

``Request Header Fields Too Large``
-----------------------------------

Problem
^^^^^^^

A query may return an error similar to the following example: 

.. code-block:: none

    Error running command: Error starting query at http://localhost:8080/v1/statement 
    returned an invalid response: JsonResponse{statusCode=431, statusMessage=Request 
    Header Fields Too Large, headers={connection=[close], content-length=[74], 
    content-type=[text/html;charset=iso-8859-1]}, hasValue=false} 
    [Error: <h1>Bad Message 431</h1><pre>reason: Request Header Fields Too Large</pre>]

Some examples of how this can happen are:

* many session properties have been added to the query
* putting a lot of information in ``clientInfo``
* large security keys being sent in the session

If such an error happens, increasing the value of ``http-server.max-request-header-size``  
can help avoid it.

Solution
^^^^^^^^

Edit ``config.properties`` for the Presto coordinator, and set the value of the 
``http-server.max-request-header-size`` configuration property to a larger size. For example:

.. code-block:: none

    http-server.max-request-header-size=5MB

See :ref:`admin/properties:\`\`http-server.max-request-header-size\`\``. 