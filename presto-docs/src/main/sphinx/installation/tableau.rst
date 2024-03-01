*************************
Web Connector for Tableau
*************************

The Presto web connector for Tableau lets users run queries from
Tableau against Presto. It implements the functions in the
`Tableau web connector API
<https://community.tableau.com/community/developers/web-data-connectors>`_.

When creating a new web data source, Tableau will ask for the URL of the web
connector. Use the following URL, replacing ``example.net:8080`` with the
hostname and port number of the Presto coordinator
(the default port is ``8080``):

.. code-block:: none

     http://example.net:8080/tableau/presto-connector.html

When Tableau first loads the Presto web connector, it will render an HTML
form. In this form you need to fill in details such as your user name,
the catalog and the schema you want to query, the data source name,
session parameters you want to set and finally the SQL query to run.
After you click ``Submit``, the query will be submitted to the Presto
coordinator and Tableau will then create an extract out of the results
retrieved from the coordinator, page by page. After Tableau is done
extracting the results of your query, you can then use this extract
for further analysis with Tableau.

.. note::
     With the Presto web connector, you can only create Tableau extracts,
     because the web connector API currently does not support the live mode.

     The web connector API only supports a subset of the data types available
     in Presto. In particular, the Tableau web connector API currently supports
     the following Tableau data types:
     ``bool``, ``date``, ``datetime``, ``float``, ``int`` and ``string``.
     Presto ``boolean`` and ``date`` types will be converted to the Tableau
     data types ``bool`` and ``date``, respectively, on the Tableau client side.
     Any other Presto types such as ``array``, ``map``, ``row``, ``double``,
     ``bigint``, etc., will be converted to a Tableau ``string`` as they do
     not map to any Tableau type.
