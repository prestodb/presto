package com.facebook.presto.operator;

import com.facebook.presto.tuple.TupleInfo;

import java.io.Closeable;
import java.util.List;

public interface OperatorFactory
        extends Closeable
{
    List<TupleInfo> getTupleInfos();

    Operator createOperator(DriverContext driverContext);

    @Override
    void close();
}
