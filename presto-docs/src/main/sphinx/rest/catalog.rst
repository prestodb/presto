================
Catalog Resource
================

.. function:: GET /v1/catalog

   Returns a list of all catalogs currently registered.

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Vary: Accept-Encoding, User-Agent
      Content-Type: application/json
      X-Content-Type-Options: nosniff
      Content-Length: 73

      ["system","tpch","tpcds","tpchstandard","hive","hive_bucketed","tpch_1"]

.. function:: POST /v1/catalog/{catalogName}

   Submits a catalog to register.

   **Example request**:

   .. sourcecode:: http

     POST /v1/catalog/tpch_1 HTTP/1.1
     Host: 127.0.0.1:8080
     Accept: */*
     X-Presto-Prefix-Url: PREFIX
     X-Presto-User: PRESTO_USER
     User-Agent: StatementClient/0.55-SNAPSHOT
     Content-Type: application/json
     Content-Length: 88

     {
          "connector.name": "tpch",
          "tpch.splits-per-node": "4"
     }

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 201 Created
      Content-Type: application/json
      Content-Length: 0

.. function:: GET /v1/catalog/{catalogName}/status

   Returns the status of a requested catalog.

   The possible result status values are ``catalog_in_use``, ``catalog_not_in_use``, ``catalog_not_found``.

   **Example request**:

   .. sourcecode:: http

      GET /v1/catalog/system/status HTTP/1.1
      Host: 127.0.0.1:8080
      Accept: */*
      X-Presto-Prefix-Url: PREFIX
      X-Presto-User: PRESTO_USER
      User-Agent: StatementClient/0.55-SNAPSHOT
      Content-Type: application/json

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/json
      X-Content-Type-Options: nosniff
      Vary: Accept-Encoding, User-Agent

      {"message":"The given catalog system is currently in use","status":"catalog_in_use"}

.. function:: DELETE /v1/catalog/{catalogName}

   Removes a catalog.

   **Example request**:

   .. sourcecode:: http

      DELETE /v1/catalog/tpch_1 HTTP/1.1
      Host: 127.0.0.1:8080
      Accept: */*
      X-Presto-Prefix-Url: PREFIX
      X-Presto-User: PRESTO_USER
      User-Agent: StatementClient/0.55-SNAPSHOT
      Content-Type: application/json

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 204 No Content

.. function:: POST /v1/catalog/{catalogName}

   Updates a catalog with new parameters.

   Possible responses are 201, 204, 409.

   **Example request**:

   .. sourcecode:: http

      PUT /v1/catalog/tpch_1 HTTP/1.1
      Host: 127.0.0.1:8080
      Accept: */*
      X-Presto-Prefix-Url: PREFIX
      X-Presto-User: PRESTO_USER
      User-Agent: StatementClient/0.55-SNAPSHOT
      Content-Type: application/json
      Content-Length: 88

      {
           "connector.name": "tpch",
           "tpch.splits-per-node": "4"
      }

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 204 No Content
