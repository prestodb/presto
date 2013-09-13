/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.spi.Split;
import com.facebook.presto.sql.planner.plan.PlanNodeId;

public interface SourceOperator
        extends Operator
{
    PlanNodeId getSourceId();

    void addSplit(Split split);

    void noMoreSplits();
}
