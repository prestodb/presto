/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.sql.planner;

import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.facebook.presto.util.IterableTransformer;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class Stage
{
    private final String stageId;
    private final PlanFragment fragment;
    private final List<Partition> partitions;
    private final List<Stage> dependencies;
    private final List<TupleInfo> tupleInfos;

    public Stage(PlanFragment fragment, List<Partition> partitions, List<Stage> dependencies)
    {
        this.fragment = checkNotNull(fragment, "fragment is null");
        this.partitions = ImmutableList.copyOf(checkNotNull(partitions, "partitions is null"));
        this.dependencies = ImmutableList.copyOf(checkNotNull(dependencies, "dependencies is null"));
        
        stageId = String.valueOf(this.fragment.getId());
        tupleInfos = ImmutableList.copyOf(IterableTransformer.on(fragment.getRoot().getOutputSymbols())
                .transform(Functions.forMap(fragment.getSymbols()))
                .transform(com.facebook.presto.sql.compiler.Type.toRaw())
                .transform(new Function<Type, TupleInfo>()
                {
                    @Override
                    public TupleInfo apply(Type input)
                    {
                        return new TupleInfo(input);
                    }
                })
                .list());
    }

    public String getStageId()
    {
        return stageId;
    }

    public List<TupleInfo> getTupleInfos()
    {
        return tupleInfos;
    }

    public PlanFragment getFragment()
    {
        return fragment;
    }

    public List<Partition> getPartitions()
    {
        return partitions;
    }

    public List<Stage> getDependencies()
    {
        return dependencies;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("fragment", fragment)
                .add("partitions", partitions)
                .add("dependencies", dependencies)
                .toString();
    }
}
