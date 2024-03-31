==============
Presto Console
==============

.. contents::
    :local:
    :backlinks: none
    :depth: 1

Overview
========

The Presto Console is a web-based UI that is included as part of a Presto server 
installation. For more information about installing Presto, see 
`Installing Presto <../installation/deployment.html#installing-presto>`_.

Configuration
=============

The default port is 8080. To configure the Presto service to use a 
different port, edit the Presto coordinator node's ``config.properties`` 
file and change the configuration property ``http-server.http.port`` to 
use a different port number. For more information, see 
`Config Properties <../installation/deployment.html#config-properties>`_.

Open the Presto Console
=======================

After starting Presto, you can access the web UI at the default port 
``8080`` using the following link in a browser:

.. code-block:: none

    http://localhost:8080

.. figure:: ../images/presto_console.png
   :align: center
