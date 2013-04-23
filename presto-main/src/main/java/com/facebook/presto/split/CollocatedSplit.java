package com.facebook.presto.split;

import com.facebook.presto.metadata.DataSourceType;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.collect.Maps;

import java.util.Map;

public class CollocatedSplit
        implements Split {

    private final Map<PlanNodeId, Split> splits;
    private final Map<PlanNodeId, Object> info;

    @JsonCreator
    public CollocatedSplit(@JsonProperty("splits") Map<PlanNodeId, Split> splits)
    {
        this.splits = splits;
        this.info = Maps.transformValues(splits, new Function<Split, Object>() {
            @Override
            public Object apply(Split split)
            {
                return split.getInfo();
            }
        });
    }

    @JsonProperty
    public Map<PlanNodeId, Split> getSplits()
    {
        return splits;
    }

    @Override
    public DataSourceType getDataSourceType()
    {
        return DataSourceType.COLLOCATED;
    }

    @Override
    public Object getInfo()
    {
        return info;
    }
}
