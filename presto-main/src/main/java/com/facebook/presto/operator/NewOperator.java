package com.facebook.presto.noperator;

import com.facebook.presto.operator.Page;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

public interface NewOperator
{
    ListenableFuture<?> NOT_BLOCKED = Futures.immediateFuture(null);

    OperatorContext getOperatorContext();

    List<TupleInfo> getTupleInfos();

    void finish();

    boolean isFinished();

    ListenableFuture<?> isBlocked();

    boolean needsInput();

    void addInput(Page page);

    Page getOutput();
}
