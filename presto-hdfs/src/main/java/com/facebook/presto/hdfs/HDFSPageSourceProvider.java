package com.facebook.presto.hdfs;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import java.util.List;

/**
 * @author jelly.guodong.jin@gmail.com
 */
public class HDFSPageSourceProvider
implements ConnectorPageSourceProvider {
    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transactionHandle, ConnectorSession session,
                                                ConnectorSplit split, List<ColumnHandle> columns) {
        return null;
    }
}
