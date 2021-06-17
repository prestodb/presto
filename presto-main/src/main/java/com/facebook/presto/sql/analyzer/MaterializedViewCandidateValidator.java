package com.facebook.presto.sql.analyzer;

import com.facebook.presto.sql.tree.DefaultTraversalVisitor;

import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GroupBy;
import com.facebook.presto.sql.tree.GroupingElement;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.google.common.collect.ImmutableSet;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class MaterializedViewCandidateValidator
        extends DefaultTraversalVisitor<Void, Void>
{
    private final ImmutableSet<String> materializedViewDefinitionColumns;
    private final Optional<ImmutableSet<GroupingElement>> materializedViewDefinitionGroupBy;
    private boolean isConvertible = true;

    public MaterializedViewCandidateValidator(
            ImmutableSet<String> materializedViewDefinitionColumns, 
            Optional<ImmutableSet<GroupingElement>> materializedViewDefinitionGroupBy)
    {
        this.materializedViewDefinitionColumns = requireNonNull(materializedViewDefinitionColumns);
        this.materializedViewDefinitionGroupBy = requireNonNull(materializedViewDefinitionGroupBy);
    }

    @Override
    protected Void visitQuerySpecification(QuerySpecification node, Void context)
    {
        node.getFrom().ifPresent(from -> process(from, context));
        process(node.getSelect(), context);
        node.getWhere().ifPresent(where -> process(where, context));

        if (materializedViewDefinitionGroupBy.isPresent() && !node.getGroupBy().isPresent()) {
            isConvertible = false;
            return null;
        }
        node.getGroupBy().ifPresent(groupBy -> process(groupBy, context));

        node.getHaving().ifPresent(having -> process(having, context));
        node.getOrderBy().ifPresent(orderBy -> process(orderBy, context));
        return null;
    }

    @Override
    protected Void visitIdentifier(Identifier node, Void context)
    {
        if (!materializedViewDefinitionColumns.contains(node.getValue())) {
            isConvertible = false;
        }
        return null;
    }

    @Override
    protected Void visitFunctionCall(FunctionCall node, Void context)
    {
        String functionCall = node.toString();

        if (!materializedViewDefinitionColumns.contains(functionCall)) {
            for (Expression argument : node.getArguments()) {
                process(argument, context);
            }
        }

        return null;
    }

    @Override
    protected Void visitGroupingElement(GroupingElement node, Void context)
    {
        if (!materializedViewDefinitionGroupBy.get().contains(node)) {
            isConvertible = false;
        }
        return null;
    }

    public boolean isConvertible()
    {
        return isConvertible;
    }
}
