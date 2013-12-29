==============
Shard Resource
==============

The Share resource returns information about Shards known by a Presto
server's local storage manager.  This service is used by Presto
servers during the coordination and execution of a Presto query.

.. function:: GET /v1/shard
   
   Returns a list of shards known to a Presto Server.

.. function:: DELETE /v1/shard/{shardUuid}

   Given a ``shardUuid`` delete a shard from a Presto Server.
   
.. function:: GET /v1/shard/{shardUuid}

   Returns information about a specific shard given a ``shardUuid``.