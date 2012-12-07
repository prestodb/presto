/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.sql.planner;

import com.facebook.presto.split.Split;
import com.google.common.base.Objects;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

public class TableScanPlanFragmentSource
        implements PlanFragmentSource
{
    private final Split split;

    @JsonCreator
    public TableScanPlanFragmentSource(@JsonProperty("split") Split split)
    {
        this.split = split;
    }

    @JsonProperty
    public Split getSplit()
    {
        return split;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("split", split)
                .toString();
    }
}
