package com.facebook.presto.sql.analyzer;

import com.facebook.presto.sql.tree.DefaultExpressionTraversalVisitor;
import com.facebook.presto.sql.tree.FunctionCall;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class WindowFunctionExtractor
        extends DefaultExpressionTraversalVisitor<Void, Void>
{
    private final ImmutableList.Builder<FunctionCall> windowFunctions = ImmutableList.builder();

    @Override
    protected Void visitFunctionCall(FunctionCall node, Void context)
    {
        if (node.getWindow().isPresent()) {
            windowFunctions.add(node);
            return null;
        }

        return super.visitFunctionCall(node, null);
    }

    public List<FunctionCall> getWindowFunctions()
    {
        return windowFunctions.build();
    }
}
