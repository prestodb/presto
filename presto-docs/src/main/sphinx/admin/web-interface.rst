=============
Web Interface
=============

Presto provides a web interface for monitoring and managing queries.
The web interface is accessible on the Presto coordinator via HTTP,
using the HTTP port number specified in the coordinator :ref:`config_properties`.

The main page has a list of queries along with information like unique query ID, query text,
query state, percentage completed, username and source from which this query originated.
The currently running queries are at the top of the page, followed by the most recently
completed or failed queries.

The possible query states are as follows:

* ``QUEUED`` -- Query has been accepted and is awaiting execution.
* ``PLANNING`` -- Query is being planned.
* ``STARTING`` -- Query execution is being started.
* ``RUNNING`` -- Query has at least one running task.
* ``BLOCKED`` -- Query is blocked and is waiting for resources (buffer space, memory, splits, etc.).
* ``FINISHING`` -- Query is finishing (e.g. commit for autocommit queries).
* ``FINISHED`` -- Query has finished executing and all output has been consumed.
* ``FAILED`` -- Query execution failed.

The ``BLOCKED`` state is normal, but if it is persistent, it should be investigated.
It has many potential causes: insufficient memory or splits, disk or network I/O bottlenecks, data skew
(all the data goes to a few workers), a lack of parallelism (only a few workers available), or computationally
expensive stages of the query following a given stage.  Additionally, a query can be in
the ``BLOCKED`` state if a client is not processing the data fast enough (common with "SELECT \*" queries).

For more detailed information about a query, simply click the query ID link.
The query detail page has a summary section, graphical representation of various stages of the
query and a list of tasks. Each task ID can be clicked to get more information about that task.

The summary section has a button to kill the currently running query. There are two visualizations
available in the summary section: task execution and timeline. The full JSON documentation containing
information and statistics about the query is available by clicking the *Raw* link. These visualizations
and other statistics can be used to analyze where time is being spent for a query.
