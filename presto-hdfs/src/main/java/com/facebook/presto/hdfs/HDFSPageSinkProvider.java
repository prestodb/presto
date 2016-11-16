package com.facebook.presto.hdfs;

import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

/**
 * @author jelly.guodong.jin@gmail.com
 */
public class HDFSPageSinkProvider
implements ConnectorPageSinkProvider {
    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session,
                                            ConnectorOutputTableHandle outputTableHandle) {
        return null;
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session,
                                            ConnectorInsertTableHandle insertTableHandle) {
        return null;
    }
}
