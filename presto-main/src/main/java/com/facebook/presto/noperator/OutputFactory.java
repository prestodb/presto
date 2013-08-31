package com.facebook.presto.noperator;

import com.facebook.presto.tuple.TupleInfo;

import java.util.List;

public interface OutputFactory
{
    NewOperatorFactory createOutputOperator(List<TupleInfo> sourceTupleInfo);
}
