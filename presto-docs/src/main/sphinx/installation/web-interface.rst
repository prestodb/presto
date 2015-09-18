======================
Web Interface
======================

Presto provides a web interface for monitoring and managing queries. This web
interface is accessible through ``example.net:8080``. Replace ``example.net:8080``
to match the host and port of the Presto coordinator as specified in :ref:`config_properties`

The main page has a list of queries along with information like unique query id, query text,
query state, percentage completed, issuing user name and source from which this query
originated. The most recently run query is at the top of the page.

For more detailed information of a query the user can simply click the link on the query id.
The query detail page has a summary section, graphical representation of various stages of the
query and a list of tasks. Each task id can be clicked to get more information about that task.
In the summary section there is a button to kill the currently running query in case the user
decides to do so for an unwanted long running query. There are couple of visualizations available
in the summary section, task execution and timeline. There is a JSON version of information and
statistics of the given query which is present in the summary section with the tag ``Raw``. These
visualizations and other statistics can be used to analyze where time is being spent for a query
that is not performing as per the expectations.

Some of the query states are that can be seen on this interface are

* FINISHED
* QUEUED
* RUNNING
* FAILED, can be further classified with error types

    * INSUFFICIENT RESOURCES
    * INTERNAL ERROR
    * USER CANCELED
    * USER ERROR
