package com.facebook.presto.split;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.spi.Split;

import java.util.List;

public interface DataStreamProvider
{
    Operator createDataStream(Split split, List<ColumnHandle> columns);
}
