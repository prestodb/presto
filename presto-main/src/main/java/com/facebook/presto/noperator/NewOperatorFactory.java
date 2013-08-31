package com.facebook.presto.noperator;

import com.facebook.presto.tuple.TupleInfo;

import java.io.Closeable;
import java.util.List;

public interface NewOperatorFactory
        extends Closeable
{
    List<TupleInfo> getTupleInfos();

    NewOperator createOperator(DriverContext driverContext);

    void close();
}
