/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.analyzer;

import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GroupBy;
import com.facebook.presto.sql.tree.GroupingElement;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.Table;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.sql.ExpressionUtils.removeGroupingElementPrefix;
import static com.facebook.presto.sql.ExpressionUtils.removeSingleColumnPrefix;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkState;

public class MaterializedViewInformationExtractor
        extends DefaultTraversalVisitor<Void, Void>
{
    private final MaterializedViewInfo materializedViewInfo = new MaterializedViewInfo();

    @Override
    protected Void visitQuerySpecification(QuerySpecification node, Void context)
    {
        if (node.getLimit().isPresent()) {
            throw new SemanticException(NOT_SUPPORTED, node, "Limit clause is not supported in query optimizer");
        }
        if (node.getHaving().isPresent()) {
            throw new SemanticException(NOT_SUPPORTED, node, "Having clause is not supported in query optimizer");
        }
        if (!node.getFrom().isPresent()) {
            throw new SemanticException(NOT_SUPPORTED, node, "Materialized view with no From clause is not supported in query optimizer");
        }
        materializedViewInfo.setBaseTable(node.getFrom().get());
        materializedViewInfo.setWhereClause(node.getWhere());
        return super.visitQuerySpecification(node, context);
    }

    protected Void visitSelect(Select node, Void context)
    {
        super.visitSelect(node, context);
        materializedViewInfo.setDistinct(node.isDistinct());
        return null;
    }

    @Override
    protected Void visitSingleColumn(SingleColumn node, Void context)
    {
        materializedViewInfo.addBaseToViewColumn(node);
        return null;
    }

    @Override
    protected Void visitAllColumns(AllColumns node, Void context)
    {
        throw new SemanticException(NOT_SUPPORTED, node, "All columns materialized view is not supported in query optimizer");
    }

    @Override
    protected Void visitGroupBy(GroupBy node, Void context)
    {
        for (GroupingElement element : node.getGroupingElements()) {
            materializedViewInfo.addGroupBy(element);
        }
        return null;
    }

    public MaterializedViewInfo getMaterializedViewInfo()
    {
        return materializedViewInfo;
    }

    public static final class MaterializedViewInfo
    {
        private final Map<Expression, Identifier> baseToViewColumnMap = new HashMap<>();
        private Optional<Relation> baseTable = Optional.empty();
        private Optional<Expression> whereClause = Optional.empty();
        private Optional<Set<Expression>> groupBy = Optional.empty();
        private boolean isDistinct;
        private Optional<Identifier> removablePrefix = Optional.empty();

        private void addBaseToViewColumn(SingleColumn singleColumn)
        {
            singleColumn = removeSingleColumnPrefix(singleColumn, removablePrefix);
            Expression key = singleColumn.getExpression();
            if (key instanceof FunctionCall && !singleColumn.getAlias().isPresent()) {
                throw new SemanticException(NOT_SUPPORTED, singleColumn, "Derived field in materialized view must have an alias");
            }
            baseToViewColumnMap.put(key, singleColumn.getAlias().orElse(new Identifier(key.toString())));
        }

        private void addGroupBy(GroupingElement groupingElement)
        {
            if (!groupBy.isPresent()) {
                groupBy = Optional.of(new HashSet<>());
            }
            groupBy.get().addAll(removeGroupingElementPrefix(groupingElement, removablePrefix).getExpressions());
        }

        private void setBaseTable(Relation baseTable)
        {
            checkState(!this.baseTable.isPresent(), "Only support single table rewrite in query optimizer");
            if (baseTable instanceof AliasedRelation) {
                removablePrefix = Optional.of(((AliasedRelation) baseTable).getAlias());
                baseTable = ((AliasedRelation) baseTable).getRelation();
            }
            if (!(baseTable instanceof Table)) {
                throw new SemanticException(NOT_SUPPORTED, baseTable, "Relation other than Table is not supported in query optimizer");
            }
            this.baseTable = Optional.of(baseTable);
            if (!removablePrefix.isPresent()) {
                removablePrefix = Optional.of(new Identifier(((Table) baseTable).getName().toString()));
            }
        }

        private void setWhereClause(Optional<Expression> whereClause)
        {
            checkState(!this.whereClause.isPresent());
            this.whereClause = whereClause;
        }

        private void setDistinct(boolean state)
        {
            isDistinct = state;
        }

        public Optional<Relation> getBaseTable()
        {
            return baseTable;
        }

        public Map<Expression, Identifier> getBaseToViewColumnMap()
        {
            return ImmutableMap.copyOf(baseToViewColumnMap);
        }

        public Optional<Expression> getWhereClause()
        {
            return whereClause;
        }

        public boolean isDistinct()
        {
            return isDistinct;
        }

        public Optional<Set<Expression>> getGroupBy()
        {
            return groupBy;
        }
    }
}
