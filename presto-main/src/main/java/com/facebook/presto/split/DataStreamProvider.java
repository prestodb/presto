package com.facebook.presto.split;

import com.facebook.presto.noperator.NewOperator;
import com.facebook.presto.noperator.OperatorContext;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Split;

import java.util.List;

public interface DataStreamProvider
{
    NewOperator createNewDataStream(OperatorContext operatorContext, Split split, List<ColumnHandle> columns);
}
