===============
Apache Superset
===============

`Apache Superset <https://superset.apache.org/>`_ is an open source data exploration tool that is part of the Apache Project. 
Follow these steps to configure Superset to query Presto. 

1. Install or deploy Superset from the `Superset documentation <https://superset.apache.org/docs/intro>`_. 

   Note: When running Superset for the first time, an error may be displayed similar to the following: 

   `validating superset/docker-compose-image-tag.yml: services.db.env_file.0 must be a string`

   See `Superset discussion 28184 <https://github.com/apache/superset/discussions/28184>`_ for more information. 

2. You may need `pyhive` to configure Superset to connect to Presto. See `Presto <https://superset.apache.org/docs/configuration/databases#presto>`_.

3. Log into Superset as described in `Log into Superset <https://superset.apache.org/docs/quickstart#3-log-into-superset>`_. 

4. In the Superset UI, select `+` in the upper right to display the drop-down menu, then select `Data`, then `Connect database`.

5. In the `Connect a database` window, select `Presto`.

6. In `SLQAlchemy URI`, enter a connection string using the following format: 

   `presto://{hostname}:{port}/{database}`

   For example, `presto://<Presto-IP-address>:8080/system`

   For more information, see the Superset documentation for connecting to `Presto <https://superset.apache.org/docs/configuration/databases#presto>`_.

7. Select `Test Connection`. If the message `Connection looks good!` is displayed, continue. 

   Note: If your Presto server is running locally, `localhost` may not resolve DNS successfully from the Superset docker-compose launched instance to the local Presto server. Replace `<Presto-IP-address>` with your system's actual IP address. 

8. Select `Connect`.

