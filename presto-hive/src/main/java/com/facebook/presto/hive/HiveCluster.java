package com.facebook.presto.hive;

/**
 * Represents a specific Hive installation
 * Note: implementations will be used as keys in a Map and will be expected to provide appropriate equals and hashCode
 * methods.
 */
public interface HiveCluster
{
    /**
     * Create a connected HiveMetastoreClient to this HiveCluster
     */
    HiveMetastoreClient createMetastoreClient();
}
