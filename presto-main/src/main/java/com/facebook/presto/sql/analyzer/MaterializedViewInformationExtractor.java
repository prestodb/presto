package com.facebook.presto.sql.analyzer;

import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.GroupBy;
import com.facebook.presto.sql.tree.GroupingElement;
import com.facebook.presto.sql.tree.SingleColumn;
import com.google.common.collect.ImmutableSet;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class MaterializedViewInformationExtractor
        extends DefaultTraversalVisitor<Void, Void>
{
    private final Set<String> materializedViewDefinitionColumns = new HashSet<>();
    private Optional<ImmutableSet<GroupingElement>> materializedViewDefinitionGroupBy = Optional.empty();

    @Override
    protected Void visitSingleColumn(SingleColumn node, Void context)
    {
        materializedViewDefinitionColumns.add(node.getExpression().toString());
        return null;
    }

    @Override
    protected Void visitGroupBy(GroupBy node, Void context)
    {
        materializedViewDefinitionGroupBy = Optional.of(ImmutableSet.copyOf(node.getGroupingElements()));
        return null;
    }

    public ImmutableSet<String> getMaterializedViewColumns()
    {
        return ImmutableSet.copyOf(materializedViewDefinitionColumns);
    }

    public Optional<ImmutableSet<GroupingElement>> getMaterializedViewGroupBy()
    {
        return materializedViewDefinitionGroupBy;
    }
}
