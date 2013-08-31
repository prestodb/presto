package com.facebook.presto.noperator;

import com.facebook.presto.operator.Page;
import com.facebook.presto.tuple.TupleInfo;

import java.util.List;

public interface NewOperator
{
    List<TupleInfo> getTupleInfos();

    void finish();

    boolean isFinished();

    boolean needsInput();

    void addInput(Page page);

    Page getOutput();
}
