package com.facebook.presto.noperator;

import com.facebook.presto.execution.TaskMemoryManager;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.tuple.TupleInfo;

import java.io.Closeable;
import java.util.List;

public interface NewOperatorFactory
        extends Closeable
{
    List<TupleInfo> getTupleInfos();

    NewOperator createOperator(OperatorStats operatorStats, TaskMemoryManager taskMemoryManager);

    void close();
}
