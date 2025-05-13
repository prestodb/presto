package com.facebook.presto.example.prac;

import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.google.inject.Inject;
import sun.plugin.viewer.LifeCycleManager;

import static com.facebook.presto.example.prac.ExamplePracTransactionHandle.INSTANCE;


/**
 * @Author LTR
 * @Date 2025/4/15 16:43
 * @注释
 */
public class ExamplePracConnector implements Connector {

    private final LifeCycleManager lifeCycleManager;
    private final ExamplePracMetadata examplePracMetaData;
    private final ExamplePracSplitManager examplePracSplitManager;
    private final ExamplePracRecordSetProvider examplePracRecordSetProvider;
    @Inject
    public ExamplePracConnector(
            LifeCycleManager lifeCycleManager,
            ExamplePracMetadata examplePracMetaData,
            ExamplePracSplitManager examplePracSplitManager,
            ExamplePracRecordSetProvider examplePracRecordSetProvider){
        this.lifeCycleManager = lifeCycleManager;
        this.examplePracSplitManager = examplePracSplitManager;
        this.examplePracMetaData = examplePracMetaData;
        this.examplePracRecordSetProvider = examplePracRecordSetProvider;

    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly) {
        return INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle) {
        return null;
    }

    @Override
    public ConnectorSplitManager getSplitManager() {
        return null;
    }
}
