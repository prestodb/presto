package com.facebook.presto.split;

import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.RecordProjectOperator;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.Split;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class RecordSetDataStreamProvider
        implements ConnectorDataStreamProvider
{
    private ConnectorRecordSetProvider recordSetProvider;

    public RecordSetDataStreamProvider(ConnectorRecordSetProvider recordSetProvider)
    {
        this.recordSetProvider = checkNotNull(recordSetProvider, "recordSetProvider is null");
    }

    @Override
    public boolean canHandle(Split split)
    {
        return recordSetProvider.canHandle(split);
    }

    @Override
    public Operator createNewDataStream(OperatorContext operatorContext, Split split, List<ColumnHandle> columns)
    {
        return new RecordProjectOperator(operatorContext, recordSetProvider.getRecordSet(split, columns));
    }
}
