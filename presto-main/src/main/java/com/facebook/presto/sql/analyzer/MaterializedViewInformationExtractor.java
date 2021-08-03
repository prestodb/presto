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
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.GroupBy;
import com.facebook.presto.sql.tree.GroupingElement;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.Table;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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
        materializedViewInfo.setWhereClause(node.getWhere());
        if (node.getFrom().isPresent()) {
            process(node.getFrom().get(), context);
        }
        process(node.getSelect(), context);
        if (node.getWhere().isPresent()) {
            process(node.getWhere().get(), context);
        }
        if (node.getGroupBy().isPresent()) {
            process(node.getGroupBy().get(), context);
        }
        if (node.getOrderBy().isPresent()) {
            process(node.getOrderBy().get(), context);
        }
        return null;
    }

    protected Void visitSelect(Select node, Void context)
    {
        super.visitSelect(node, context);
        materializedViewInfo.setDistinct(node.isDistinct());
        return null;
    }

    @Override
    protected Void visitAliasedRelation(AliasedRelation node, Void context)
    {
        materializedViewInfo.setBaseTableAlias(node.getAlias());
        return process(node.getRelation(), context);
    }

    @Override
    protected Void visitRelation(Relation node, Void context)
    {
        if (!(node instanceof Table)) {
            throw new SemanticException(NOT_SUPPORTED, node, "Relation other than Table is not supported in query optimizer");
        }
        if (materializedViewInfo.getBaseTable().isPresent()) {
            throw new SemanticException(NOT_SUPPORTED, node, "Only support single table rewrite in query optimizer");
        }
        if (!materializedViewInfo.getBaseTableAlias().isPresent()) {
            materializedViewInfo.setBaseTableAlias(new Identifier(((Table) node).getName().getSuffix()));
        }
        materializedViewInfo.setBaseTable(Optional.of(node));
        return null;
    }

    @Override
    protected Void visitSingleColumn(SingleColumn node, Void context)
    {
        Expression baseColumn = node.getExpression();
        Expression columnAlias = baseColumn;
        if (node.getAlias().isPresent()) {
            columnAlias = node.getAlias().get();
        }
        if (baseColumn instanceof Identifier) {
            DereferenceExpression rewrittenDereferenceExpression = new DereferenceExpression(materializedViewInfo.getBaseTableAlias().get(), (Identifier) baseColumn);
            materializedViewInfo.addBaseToViewColumn(rewrittenDereferenceExpression, columnAlias);
        }
        else if(baseColumn instanceof DereferenceExpression) {
            DereferenceExpression baseDereferenceExpression = (DereferenceExpression) baseColumn;
            if (!baseDereferenceExpression.getBase().equals(materializedViewInfo.getBaseTableAlias().get())) {
                throw new IllegalStateException("Illegal alias dereference expression in MV Definition");
            }
            DereferenceExpression rewrittenDereferenceExpression = new DereferenceExpression(materializedViewInfo.getBaseTableAlias().get(), baseDereferenceExpression.getField());
            materializedViewInfo.addBaseToViewColumn(rewrittenDereferenceExpression, columnAlias);
        }
        else {
            materializedViewInfo.addBaseToViewColumn(baseColumn, columnAlias);
        }
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
        materializedViewInfo.setGroupBy(Optional.of(ImmutableSet.copyOf(node.getGroupingElements())));
        return null;
    }

    public MaterializedViewInfo getMaterializedViewInfo()
    {
        return materializedViewInfo;
    }

    public static final class MaterializedViewInfo
    {
        private final Map<Expression, Expression> baseToViewColumnMap = new HashMap<>();
        private Optional<Relation> baseTable = Optional.empty();
        private Optional<Identifier> baseTableAlias = Optional.empty();
        private Optional<Expression> whereClause = Optional.empty();
        private Optional<Set<GroupingElement>> groupBy = Optional.empty();
        private boolean isDistinct;

        private void addBaseToViewColumn(Expression key, Expression value)
        {
            baseToViewColumnMap.put(key, value);
        }

        private void setGroupBy(Optional<Set<GroupingElement>> groupBy)
        {
            checkState(!this.groupBy.isPresent());
            this.groupBy = groupBy;
        }

        private void setBaseTable(Optional<Relation> baseTable)
        {
            checkState(!this.baseTable.isPresent());
            this.baseTable = baseTable;
        }

        private void setBaseTableAlias(Identifier baseTableAlias)
        {
            this.baseTableAlias = Optional.of(baseTableAlias);
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

        public Optional<Identifier> getBaseTableAlias()
        {
            return baseTableAlias;
        }

        public Map<Expression, Expression> getBaseToViewColumnMap()
        {
            return ImmutableMap.copyOf(baseToViewColumnMap);
        }

        public Optional<Set<GroupingElement>> getGroupBy()
        {
            return groupBy;
        }

        public Optional<Expression> getWhereClause()
        {
            return whereClause;
        }

        public boolean isDistinct()
        {
            return isDistinct;
        }
    }
}
