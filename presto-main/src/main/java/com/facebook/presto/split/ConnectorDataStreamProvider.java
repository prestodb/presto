package com.facebook.presto.split;

import com.facebook.presto.operator.NewOperator;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Split;

import java.util.List;

public interface ConnectorDataStreamProvider
{
    boolean canHandle(Split split);

    NewOperator createNewDataStream(OperatorContext operatorContext, Split split, List<ColumnHandle> columns);
}
