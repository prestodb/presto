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

For more detailed information about a query, simply click the query ID link.
The query detail page has a summary section, graphical representation of various stages of the
query and a list of tasks. Each task ID can be clicked to get more information about that task.

The summary section has a button to kill the currently running query. There are two visualizations
available in the summary section: task execution and timeline. The full JSON documentation containing
information and statistics about the query is available by clicking the *Raw* link. These visualizations
and other statistics can be used to analyze where time is being spent for a query.
