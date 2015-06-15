*************************
Web Connector for Tableau
*************************

Presto web connector for Tableau implements the functions in the Tableau web
connector API and lets users run queries from Tableau against Presto. You can
get more info about the Tableau web connector API at
`<http://community.tableau.com/community/developers/web-data-connectors>`_.

When creating a new web data source Tableau will ask for the URL of the web
connector, which is
``http://[presto_coordinator_host]:[port]/tableu/presto-connector.html``
where ``presto_coordinator_host`` is the hostname that Presto coordinator is
running on, ``port`` is 8080 by default. When Tableau first loads the Presto
web connector it will render an HTML form. In this form you need to fill in
details such as your user name, the catalog and the schema you want to query,
the data source name, and finally the SQL query to run. After you click
``Submit`` the query will be submitted to the Presto coordinator and Tableau
will then create an extract out of the results retrieved from the coordinator
page by page. After Tableau is done extracting the results of your query you
can then use this extract for further analysis with Tableau.

.. note::
     With Presto web connector you can only create Tableau extracts as the web
     connector API currently doesn't support the live mode.


