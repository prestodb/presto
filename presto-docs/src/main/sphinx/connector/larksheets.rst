=======================
Lark Sheets connector
=======================

The Lark Sheets connector allows reading `Lark Sheets <https://www.larksuite.com/en_us/>`_ spreadsheets in Presto.

Lark App
--------

The connector requires a Lark app in order to access the Lark Sheets API.

1. Create a Lark app by clicking the **Create Custom App** button at
   `Lark Open Platform <https://open.larksuite.com/app>`_ (for global users) or
   `Feishu Open Platform <https://open.feishu.cn/app>`_ (for Chinese users).
   Choose ``Custom App`` when creating the app. Get and remember ``app id`` and ``app secret`` from
   **Credentials and Basic Info** page.
2. Grant ``View, comment, and export Sheets`` permission to the app at **Permission & Scopes** page.
   And publish a new release for the app at **App Release** page to make the permissions take effect.

Configuration
-------------

Create ``etc/catalog/larksheets.properties``
to mount the Lark Sheets connector as the ``larksheets`` catalog,
replacing the properties as appropriate:

.. code-block:: text

    connector.name=lark-sheets
    app-domain=FEISHU
    app-id=example_app_id
    app-secret-file=/path/to/app-secret.json

Create ``app-secret.json`` as the example below:

.. code-block:: json

    {"app-secret": "abcdefghijklmnopqrstuvwxyzabcdef"}

The following configuration properties are available:

=================================== ========================================================================
Property Name                       Description
=================================== ========================================================================
``app-domain``                      Use ``LARK`` if created at `Lark Open Platform`, or ``FEISHU`` otherwise
``app-id``                          Your app id, available at `Lark Open Platform` or `Feishu Open Platform`
``app-secret-file``                 Path to the JSON file that contains your app secret
=================================== ========================================================================

Query Model
------------------

In the connector, a Lark spreadsheet is mapped to a Presto schema, and each sheet
inside the spreadsheet is mapped to a Presto table.

Create a spreadsheet-schema mapping by executing a query like:

.. code-block:: sql

    CREATE SCHEMA my_ss WITH (TOKEN = 'shtcnBf5pg4BNSkwV2Ku5xwW9Pf')

The ``token`` property is taken from the last path segment of the spreadsheet url, using the pattern
``https://{lark_site_domain}/sheets/{token}?sheet={sheet_id}``. For example,
``shtcnBf5pg4BNSkwV2Ku5xwW9Pf`` is the token of the spreadsheet at
``https://test-ch80md45anra.feishu.cn/sheets/shtcnBf5pg4BNSkwV2Ku5xwW9Pf?sheet=MT1p4I``

After that, all sheets (tabs) of the spreadsheet are automatically mapped to tables in the schema.

.. code-block:: sql

    SHOW TABLES FROM my_ss

For each table, its name is taken from the sheet title; its columns are inferred from the head row
(the first row of the sheet). Sheets metadata could be listed through the query as:

.. code-block:: sql

    SELECT * FROM my_ss."$sheets"

In a query statement, a table can be specified in such ways:

.. code-block:: sql

    -- by sheet title
    SELECT * FROM my_ss."number_text"

    -- by sheet index
    SELECT * FROM my_ss."$0"

    -- by sheet id
    SELECT * FROM my_ss."@MT1p4I"

As for column infer, columns that have blank value in the head row are ignored.
A sheet with duplicate column names can not be queried.

Security Model
--------------

Only the spreadsheet that enables **Link Sharing** can be queried.

The schema is visible and manipulative only to its creator by default.
To make the schema visible to others, create it as:

.. code-block:: sql

    CREATE SCHEMA my_ss WITH (TOKEN = 'shtcnBf5pg4BNSkwV2Ku5xwW9Pf', PUBLIC = true)
