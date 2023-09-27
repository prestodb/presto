=======================
Redis HBO Provider
=======================

Redis HBO Provider allows loading a custom configured Redis Client for storing/retrieving Historical Stats. The Redis client is stateful and is based on
`Lettuce <https://github.com/lettuce-io/lettuce-core>`_ . Both RedisClient and RedisClusterClient are supported, RedisClusterAsyncCommandsFactory is meant to be extended by the user for custom configurations


Configuration
-------------

Create ``etc/catalog/hbo-provider.properties`` to mount the Redis HBO Provider Plugin, replacing the properties as appropriate:

Configuration properties
------------------------

The following configuration properties are available:


============================================ =====================================================================
Property Name                                Description
============================================ =====================================================================
``coordinator``                              Boolean property whether Presto server is a coordinator
``hbo.redis-provider.server_uri``            Redis Server URI
``hbo.redis-provider.total-fetch-timeoutms`` Maximum timeout in ms for Redis fetch requests
``hbo.redis-provider.total-set-timeoutms``   Maximum timeout in ms for Redis set requests
``hbo.redis-provider.default-ttl-seconds``   TTL in seconds of the Redis data to be stored
``hbo.redis-provider.enabled``               Boolean property whether this plugin is enabled in production
``credentials-path``                         Path for Redis credentials
``hbo.redis-provider.cluster-mode-enabled``  Boolean property whether cluster mode is enabled
============================================ =====================================================================

Credentials
-----------

The plugin requires the Redis Server URI property ``hbo.redis-provider.server_uri`` in order to access Redis.
Based on your custom Redis deployment you may need to add additional credentials.

Local Test setup
------------------------

Set up local Redis Cluster with the guideline of `Redis <https://github.com/lettuce-io/lettuce-core>`_.

Add ``../redis-hbo-provider/pom.xml,\`` to ``presto-main/etc/config.properties``.

Add the following config file in ``presto-main/etc/hbo-provider.properties``

.. code-block:: text

    coordinator=true
    hbo.redis-provider.enabled=true
    hbo.redis-provider.total-fetch-timeoutms=5000
    hbo.redis-provider.total-set-timeoutms=5000
    hbo.redis-provider.default-ttl-seconds=4320000
    hbo.redis-provider.cluster-mode-enabled=true
    hbo.redis-provider.server_uri=redis://localhost:7001/

Production Setup
------------------------

1. You can place the plugin JARs in the production's plugins directory.

2. Alternatively, follow this method to ensure that the plugin is loaded during the Presto build.
Steps to register the plugin in production

1. Add the following to register the plugin in <fileSets> in presto-server/src/main/assembly/presto.xml

.. code-block:: text

    <fileSet>
       <directory>${project.build.directory}/dependency/redis-hbo-provider-${project.version}</directory>
       <outputDirectory>plugin/redis-hbo-provider</outputDirectory>
    </fileSet>

2. Add META-INF.services file in ``redis-hbo-provider/src/main/resources`` with the Plugin entry class com.facebook.presto.statistic.RedisProviderPlugin

3. Add dependency on the module in ``presto-server/pom.xml``

.. code-block:: text

    <dependency>
        <groupId>com.facebook.presto</groupId>
        <artifactId>redis-hbo-provider</artifactId>
        <version>${project.version}</version>
        <type>zip</type>
        <scope>provided</scope>
    </dependency>

4. You can add your custom Redis client connection login in ``com.facebook.presto.statistic.RedisClusterAsyncCommandsFactory``, just make sure that the AsyncCommands are provided properly
