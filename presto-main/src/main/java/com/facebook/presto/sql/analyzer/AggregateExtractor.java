package com.facebook.presto.sql.analyzer;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.FunctionCall;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

public class AggregateExtractor
        extends DefaultTraversalVisitor<Void, Void>
{
    private final Metadata metadata;

    private final ImmutableList.Builder<FunctionCall> aggregates = ImmutableList.builder();

    public AggregateExtractor(Metadata metadata)
    {
        Preconditions.checkNotNull(metadata, "metadata is null");

        this.metadata = metadata;
    }

    @Override
    protected Void visitFunctionCall(FunctionCall node, Void context)
    {
        if (metadata.isAggregationFunction(node.getName()) && !node.getWindow().isPresent()) {
            aggregates.add(node);
            return null;
        }

        return super.visitFunctionCall(node, null);
    }

    public List<FunctionCall> getAggregates()
    {
        return aggregates.build();
    }
}
