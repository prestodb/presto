package com.facebook.presto.dynamo;

import com.facebook.presto.baseplugin.BaseProvider;
import com.facebook.presto.baseplugin.BaseRecordSet;
import com.facebook.presto.baseplugin.BaseRecordSetProvider;
import com.facebook.presto.baseplugin.BaseSplit;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.inject.Inject;

import java.util.List;
import java.util.Optional;

/**
 * Created by amehta on 7/25/16.
 */
public class DynamoRecordSetProvider extends BaseRecordSetProvider {
    @Inject
    public DynamoRecordSetProvider(BaseProvider baseProvider) {
        super(baseProvider);
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorSplit split, List<? extends ColumnHandle> columns) {
        DynamoQuery dynamoQuery = new DynamoQuery((BaseSplit) split, session, columns);
        return new BaseRecordSet(dynamoQuery, getBaseProvider());
    }
}
