package com.facebook.presto.baseplugin;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.inject.Inject;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Created by amehta on 6/13/16.
 */
public class BaseRecordSetProvider implements ConnectorRecordSetProvider {
    private final BaseProvider baseProvider;

    @Inject
    public BaseRecordSetProvider(BaseProvider baseProvider){
        this.baseProvider = requireNonNull(baseProvider, "baseProvider is null");
    }

    public BaseProvider getBaseProvider() {
        return baseProvider;
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorSplit split, List<? extends ColumnHandle> columns){
        return new BaseRecordSet(new BaseQuery((BaseSplit) split, session, columns), baseProvider);
    }
}
