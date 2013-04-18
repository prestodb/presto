/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.spi.Split;

public interface SourceOperator
        extends Operator
{
    void addSplit(Split split);

    void noMoreSplits();
}
