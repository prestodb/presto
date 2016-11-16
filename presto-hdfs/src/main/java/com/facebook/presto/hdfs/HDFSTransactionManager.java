package com.facebook.presto.hdfs;

import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author jelly.guodong.jin@gmail.com
 */
public class HDFSTransactionManager {
    private final ConcurrentMap<ConnectorTransactionHandle, ConnectorMetadata> transactions = new ConcurrentHashMap<>();

    public ConnectorMetadata get(ConnectorTransactionHandle transactionHandle) {
        return transactions.get(transactionHandle);
    }

    public ConnectorMetadata remove(ConnectorTransactionHandle transactionHandle) {
        return transactions.remove(transactionHandle);
    }

    public void put(ConnectorTransactionHandle transactionHandle, ConnectorMetadata metadata) {
        transactions.putIfAbsent(transactionHandle, metadata);
    }
}
