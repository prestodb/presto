package com.facebook.presto.split;

import com.facebook.presto.ingest.RecordProjectOperator;
import com.facebook.presto.noperator.NewOperator;
import com.facebook.presto.noperator.NewRecordProjectOperator;
import com.facebook.presto.noperator.OperatorContext;
import com.facebook.presto.operator.Operator;
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
    public Operator createDataStream(Split split, List<ColumnHandle> columns)
    {
        return new RecordProjectOperator(recordSetProvider.getRecordSet(split, columns));
    }

    @Override
    public NewOperator createNewDataStream(OperatorContext operatorContext, Split split, List<ColumnHandle> columns)
    {
        return new NewRecordProjectOperator(operatorContext, recordSetProvider.getRecordSet(split, columns));
    }
}
