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
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.GroupBy;
import com.facebook.presto.sql.tree.GroupingElement;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.SingleColumn;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class MaterializedViewInformationExtractor
        extends DefaultTraversalVisitor<Void, MaterializedViewInformationExtractor.MaterializedViewInfo>
{
    @Override
    protected Void visitQuerySpecification(QuerySpecification node, MaterializedViewInfo materializedViewInfo)
    {
        super.visitQuerySpecification(node, materializedViewInfo);
        if (node.getLimit().isPresent()) {
            materializedViewInfo.setLimitClausePresented(true);
        }
        return null;
    }

    @Override
    protected Void visitAliasedRelation(AliasedRelation node, MaterializedViewInfo materializedViewInfo)
    {
        materializedViewInfo.setTableAliasPresented(true);
        return null;
    }

    @Override
    protected Void visitRelation(Relation node, MaterializedViewInfo materializedViewInfo)
    {
        materializedViewInfo.setMaterializedViewDefinitionRelation(Optional.of(node));
        return null;
    }

    @Override
    protected Void visitSingleColumn(SingleColumn node, MaterializedViewInfo materializedViewInfo)
    {
        Expression baseColumnName = node.getExpression();
        Optional<Identifier> baseColumnAlias = node.getAlias();
        Identifier viewDerivedColumnName = baseColumnAlias.orElse(new Identifier(baseColumnName.toString()));

        materializedViewInfo.putInBaseToViewColumnMap(baseColumnName, viewDerivedColumnName);
        return null;
    }

    @Override
    protected Void visitGroupBy(GroupBy node, MaterializedViewInfo materializedViewInfo)
    {
        materializedViewInfo.setMaterializedViewDefinitionGroupBy(Optional.of(ImmutableSet.copyOf(node.getGroupingElements())));
        return null;
    }

    // Currently the following two methods will save the entire expression in the WHERE clause (logicalExpression or ComparisonExpression)
    // TODO: Implement methods that save WHERE clause information in a better way which enable implication comparison (a's WHERE clause implies b's WHERE clause)
    @Override
    protected Void visitLogicalBinaryExpression(LogicalBinaryExpression node, MaterializedViewInfo materializedViewInfo)
    {
        materializedViewInfo.setMaterializedViewDefinitionWhere(Optional.of(node));
        return null;
    }

    @Override
    protected Void visitComparisonExpression(ComparisonExpression node, MaterializedViewInfo materializedViewInfo)
    {
        materializedViewInfo.setMaterializedViewDefinitionWhere(Optional.of(node));
        return null;
    }

    public static final class MaterializedViewInfo
    {
        private final Map<Expression, Identifier> baseToViewColumnMap = new HashMap<>();
        private Optional<Relation> materializedViewDefinitionRelation = Optional.empty();
        private Optional<Expression> materializedViewDefinitionWhere = Optional.empty();
        private Optional<ImmutableSet<GroupingElement>> materializedViewDefinitionGroupBy = Optional.empty();
        private boolean isLimitClausePresented;
        private boolean isTableAliasPresented;

        private void putInBaseToViewColumnMap(Expression key, Identifier value)
        {
            baseToViewColumnMap.put(key, value);
        }

        private void setMaterializedViewDefinitionGroupBy(Optional<ImmutableSet<GroupingElement>> materializedViewDefinitionGroupBy)
        {
            this.materializedViewDefinitionGroupBy = materializedViewDefinitionGroupBy;
        }

        private void setMaterializedViewDefinitionRelation(Optional<Relation> materializedViewDefinitionRelation)
        {
            this.materializedViewDefinitionRelation = materializedViewDefinitionRelation;
        }

        private void setMaterializedViewDefinitionWhere(Optional<Expression> materializedViewDefinitionWhere)
        {
            this.materializedViewDefinitionWhere = materializedViewDefinitionWhere;
        }

        private void setLimitClausePresented(boolean state)
        {
            isLimitClausePresented = state;
        }

        private void setTableAliasPresented(boolean state)
        {
            isTableAliasPresented = state;
        }

        public Optional<Relation> getMaterializedViewDefinitionRelation()
        {
            return materializedViewDefinitionRelation;
        }

        public ImmutableMap<Expression, Identifier> getBaseToViewColumnMap()
        {
            return ImmutableMap.copyOf(baseToViewColumnMap);
        }

        public Optional<ImmutableSet<GroupingElement>> getMaterializedViewDefinitionGroupBy()
        {
            return materializedViewDefinitionGroupBy;
        }

        public Optional<Expression> getMaterializedViewDefinitionWhere()
        {
            return materializedViewDefinitionWhere;
        }

        public boolean isLimitClausePresented()
        {
            return isLimitClausePresented;
        }

        public boolean isTableAliasPresented()
        {
            return isTableAliasPresented;
        }
    }
}
