/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.sql.planner;

import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.operator.Operator;

import java.util.List;

public interface PlanFragmentSourceProvider
{
    Operator createDataStream(PlanFragmentSource source, List<ColumnHandle> columns);
}
