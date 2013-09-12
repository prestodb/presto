package com.facebook.presto.operator;

import com.facebook.presto.tuple.TupleInfo;

import java.util.List;

public interface OutputFactory
{
    NewOperatorFactory createOutputOperator(int operatorId, List<TupleInfo> sourceTupleInfo);
}
