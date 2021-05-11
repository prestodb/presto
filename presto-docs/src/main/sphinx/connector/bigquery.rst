==================
BigQuery Connector
==================

The BigQuery connector allows querying the data stored in `BigQuery
<https://cloud.google.com/bigquery/>`_. This can be used to join data between
different systems like BigQuery and Hive. The connector uses the `BigQuery
Storage API <https://cloud.google.com/bigquery/docs/reference/storage/>`_ to
read the data from the tables.

Beta Disclaimer
---------------

The BigQuery Storage API and this connector are in Beta and are subject to change.

Changes may include, but are not limited to:

* Type conversion
* Partitioning
* Parameters

BigQuery Storage API
--------------------

The Storage API streams data in parallel directly from BigQuery via gRPC without
using Google Cloud Storage as an intermediary.
It has a number of advantages over using the previous export-based read flow
that should generally lead to better read performance:

**Direct Streaming**

    It does not leave any temporary files in Google Cloud Storage. Rows are read
    directly from BigQuery servers using an Avro wire format.

**Column Filtering**

    The new API allows column filtering to only read the data you are interested in.
    `Backed by a columnar datastore <https://cloud.google.com/blog/big-data/2016/04/inside-capacitor-bigquerys-next-generation-columnar-storage-format>`_,
    it can efficiently stream data without reading all columns.

**Dynamic Sharding**

    The API rebalances records between readers until they all complete. This means
    that all Map phases will finish nearly concurrently. See this blog article on
    `how dynamic sharding is similarly used in Google Cloud Dataflow
    <https://cloud.google.com/blog/big-data/2016/05/no-shard-left-behind-dynamic-work-rebalancing-in-google-cloud-dataflow>`_.

Requirements
------------

Enable the BigQuery Storage API
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Follow `these instructions <https://cloud.google.com/bigquery/docs/reference/storage/#enabling_the_api>`_.

Authentication
^^^^^^^^^^^^^^

**On GCE/Dataproc** the authentication is taken from the machine's role.

**Outside GCE/Dataproc** you have 3 options:

* Use a service account JSON key and ``GOOGLE_APPLICATION_CREDENTIALS`` as
  described `here <https://cloud.google.com/docs/authentication/getting-started>`_.
* Set ``bigquery.credentials`` in the catalog properties file.
  It should contain the contents of the JSON file, encoded using base64.
* Set ``bigquery.credentials-file`` in the catalog properties file.
  It should point to the location of the JSON file.

Configuration
-------------

To configure the BigQuery connector, create a catalog properties file in
``etc/catalog`` named, for example, ``bigquery.properties``, to mount the
BigQuery connector as the ``bigquery`` catalog. Create the file with the
following contents, replacing the connection properties as appropriate for
your setup:

.. code-block:: none

    connector.name=bigquery
    bigquery.project-id=<your Google Cloud Platform project id>

Multiple GCP Projects
^^^^^^^^^^^^^^^^^^^^^

The BigQuery connector can only access a single GCP project.Thus, if you have
data in multiple GCP projects, You need to create several catalogs, each
pointingto a different GCP project. For example, if you have two GCP projects,
one for the sales and one for analytics, you can create two properties files in
``etc/catalog`` named ``sales.properties`` and ``analytics.properties``, both
having ``connector.name=bigquery`` but with different ``project-id``. This will
create the two catalogs, ``sales`` and ``analytics`` respectively.

Configuring Partitioning
^^^^^^^^^^^^^^^^^^^^^^^^

By default the connector creates one partition per 400MB in the table being
read (before filtering). This should roughly correspond to the maximum number
of readers supported by the BigQuery Storage API. This can be configured
explicitly with the ``bigquery.parallelism`` property. BigQuery may limit the
number of partitions based on server constraints.

Reading From Views
^^^^^^^^^^^^^^^^^^

The connector has a preliminary support for reading from `BigQuery views
<https://cloud.google.com/bigquery/docs/views-intro>`_. Please note there are
a few caveats:

* BigQuery views are not materialized by default, which means that the
  connector needs to materialize them before it can read them. This process
  affects the read performance.
* The materialization process can also incur additional costs to your BigQuery bill.
* By default, the materialized views are created in the same project and
  dataset. Those can be configured by the optional ``bigquery.view-materialization-project``
  and ``bigquery.view-materialization-dataset`` properties, respectively. The
  service account must have write permission to the project and the dataset in
  order to materialize the view.
* Reading from views is disabled by default. In order to enable it, set the
  ``bigquery.views-enabled`` configuration property to ``true``.

Configuration Properties
^^^^^^^^^^^^^^^^^^^^^^^^

All configuration properties are optional.

========================================= ============================================================== ==============================================
Property                                  Description                                                    Default
========================================= ============================================================== ==============================================
``bigquery.project-id``                   Google Cloud project ID                                        Taken from the service account
``bigquery.parent-project``               Google Cloud parent project ID                                 Taken from the service account
``bigquery.parallelism``                  The number of partitions to split the data                     The number of executors
``bigquery.views-enabled``                Enable BigQuery connector to read views.                       ``false``
``bigquery.view-materialization-project`` The project where the materialized view is going to be created The view's project
``bigquery.view-materialization-dataset`` The dataset where the materialized view is going to be created The view's dataset
``bigquery.max-read-rows-retries``        The number of retries in case of retryable server issues       ``3``
``bigquery.credentials-key``              credentials key (base64 encoded)                               None. See `authentication <#authentication>`_
``bigquery.credentials-file``             JSON credentials file path                                     None. See `authentication <#authentication>`_
========================================= ============================================================== ==============================================

Data Types
----------

With a few exceptions, all BigQuery types are mapped directly to their Presto
counterparts. Here are all the mappings:

=============  ============================ =============================================================================================================
BigQuery       Presto                       Notes
=============  ============================ =============================================================================================================
``BOOLEAN``    ``BOOLEAN``
``BYTES``      ``VARBINARY``
``DATE``       ``DATE``
``DATETIME``   ``TIMESTAMP``
``FLOAT``      ``DOUBLE``
``GEOGRAPHY``  ``VARCHAR``                  In `Well-known text (WKT) <https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry>`_ format
``INTEGER``    ``BIGINT``
``NUMERIC``    ``DECIMAL(38,9)``
``RECORD``     ``ROW``
``STRING``     ``VARCHAR``
``TIME``       ``TIME_WITH_TIME_ZONE``      Time zone is UTC
``TIMESTAMP``  ``TIMESTAMP_WITH_TIME_ZONE`` Time zone is UTC
=============  ============================ =============================================================================================================

FAQ
---

What is the Pricing for the Storage API?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

See the `BigQuery pricing documentation
<https://cloud.google.com/bigquery/pricing#storage-api>`_.

