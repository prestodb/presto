***************
Presto REST API
***************

This chapter defines the Presto REST API. Presto uses REST for all
communication within a Presto installation. JSON-based REST services
facilitate communication between the client and the Presto coordinator
as well as for communicate between a Presto coordinator and multiple
Presto workers. In this chapter you will find detailed descriptions
of the APIs offered by Presto as well as example requests and
responses.

.. toctree::
    :maxdepth: 1

    rest/node
    rest/query
    rest/stage
    rest/statement
    rest/task

REST API Overview
-----------------

In Presto, everything is exposed as a REST API in Presto and HTTP is
the method by which all component communicate with each other.

The Presto REST API contains several, high-level resources that
correspond to the components of a Presto installation.


Query Resource

    The query resource takes a SQL query. It is available at the path
    ``/v1/query`` and accepts several HTTP methods.

Node Resource

    The node resource returns information about worker nodes in a
    Presto installation. It is available at the path ``/v1/node``.

Stage Resource

    When a Presto coordinator receives a query it creates distribute
    system of stages which collaborate with one another to execute a
    query. The Stage resource is used by the coordinator to create a
    network of corresponding stages. It is also used by stages to
    coordinate with one another.

Statement Resource

    This is the standard resource used by the client to execute a
    statement. When executing a statement, the Presto client will
    call this resource repeatedly to get the status of an ongoing
    statement execution as well as the results of a completed
    statement.

Task Resource

    A stage contains a number of components, one of which is a
    task. This resource is used by internal components to coordinate
    the execution of stages.
