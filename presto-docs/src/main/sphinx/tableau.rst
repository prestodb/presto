***************************************
Presto Web Connector for Tableau (Beta)
***************************************

Presto web connector for Tableau implements the functions in the Tableau
web connector API and enables running queries from Tableau against Presto. Tableau's
web connector API is only available in the beta v9 release and can be enabled
with a specific product key. After enabling the web connector support you can use
the Presto web connector to create a new web data source.

When you are creating a new web data source Tableau will ask you to enter the URL of the web connector.
You should enter the URL as ``http://presto_master_host:8080/presto-connector.html`` where
``presto_master_host`` is the name of the host that Presto master is running on. When Tableau
loads the Presto web connector it will render an HTML form. In this form you need to fill in details
such as your user name, the catalog and the schema that you want to query, the data source name, and finally
the SQL query to run. After you click ``Submit`` the query will be submitted to the Presto master
and Tableau will then create an extract out of the results retrieved from the master page by page.
After Tableau is done extracting the result of your query you can then use this extract for further
analysis with Tableau.

.. note::
     With Presto web connector you can only create Tableau extracts as the web connector API
     currently doesn't support the live mode.


