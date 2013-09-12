package com.facebook.presto.operator;

import com.facebook.presto.tuple.TupleInfo;

import java.io.Closeable;
import java.util.List;

public interface NewOperatorFactory
        extends Closeable
{
    List<TupleInfo> getTupleInfos();

    NewOperator createOperator(DriverContext driverContext);

    @Override
    void close();
}
