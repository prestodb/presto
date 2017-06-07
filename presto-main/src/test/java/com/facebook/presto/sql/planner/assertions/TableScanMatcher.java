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
package com.facebook.presto.sql.planner.assertions;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.predicate.AllExpression;
import com.facebook.presto.spi.predicate.AndExpression;
import com.facebook.presto.spi.predicate.DomainExpression;
import com.facebook.presto.spi.predicate.NoneExpression;
import com.facebook.presto.spi.predicate.NotExpression;
import com.facebook.presto.spi.predicate.OrExpression;
import com.facebook.presto.spi.predicate.TupleExpression;
import com.facebook.presto.spi.predicate.TupleExpressionVisitor;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;

import java.util.Map;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

final class TableScanMatcher
        implements Matcher
{
    private final String expectedTableName;
    private final TupleExpression<String> expectedConstraint;

    TableScanMatcher(String expectedTableName)
    {
        this.expectedTableName = requireNonNull(expectedTableName, "expectedTableName is null");
        expectedConstraint = new NoneExpression<String>();
    }

    public TableScanMatcher(String expectedTableName, TupleExpression<String> expectedConstraint)
    {
        this.expectedTableName = requireNonNull(expectedTableName, "expectedTableName is null");
        this.expectedConstraint = requireNonNull(expectedConstraint, "expectedConstraint is null");
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        return node instanceof TableScanNode;
    }

    @Override
    public MatchResult detailMatches(PlanNode node, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        checkState(shapeMatches(node), "Plan testing framework error: shapeMatches returned false in detailMatches in %s", this.getClass().getName());

        TableScanNode tableScanNode = (TableScanNode) node;
        TableMetadata tableMetadata = metadata.getTableMetadata(session, tableScanNode.getTable());
        String actualTableName = tableMetadata.getTable().getTableName();
        return new MatchResult(
                expectedTableName.equalsIgnoreCase(actualTableName) &&
                        domainMatches(tableScanNode, session, metadata));
    }

    private boolean domainMatches(TableScanNode tableScanNode, Session session, Metadata metadata)
    {
        if (expectedConstraint instanceof NoneExpression) {
            return true;
        }

        TupleExpression<ColumnHandle> actualConstraint = tableScanNode.getCurrentConstraint();
        if (!(expectedConstraint instanceof NoneExpression) && actualConstraint instanceof NoneExpression) {
            return false;
        }

        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableScanNode.getTable());
        return expectedConstraint.accept(new Visitor(columnHandles), actualConstraint);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .omitNullValues()
                .add("expectedTableName", expectedTableName)
                .add("expectedConstraint", expectedConstraint)
                .toString();
    }

    private class Visitor
            implements TupleExpressionVisitor<Boolean, TupleExpression<ColumnHandle>, String>
    {
        private final Map<String, ColumnHandle> columnHandles;

        public Visitor(Map<String, ColumnHandle> columnHandles)
        {
            this.columnHandles = columnHandles;
        }

        @Override
        public Boolean visitDomainExpression(DomainExpression<String> expression, TupleExpression<ColumnHandle> context)
        {
            if (!columnHandles.containsKey(expression.getColumn())) {
                return false;
            }
            if (!(context instanceof DomainExpression)) {
                return false;
            }
            DomainExpression<ColumnHandle> actualConstraint = (DomainExpression) context;

            ColumnHandle columnHandle = columnHandles.get(expression.getColumn());
            if (!actualConstraint.getColumn().equals(columnHandle)) {
                return false;
            }
            return expression.getDomain().contains(actualConstraint.getDomain());
        }

        @Override
        public Boolean visitAndExpression(AndExpression<String> expression, TupleExpression<ColumnHandle> context)
        {
            if (!(context instanceof AndExpression)) {
                return false;
            }
            AndExpression andExpression = ((AndExpression) context);
            return expression.getLeftExpression().accept(this, andExpression.getLeftExpression()) && expression.getRightExpression().accept(this,
                    andExpression.getRightExpression());
        }

        @Override
        public Boolean visitOrExpression(OrExpression<String> expression, TupleExpression<ColumnHandle> context)
        {
            if (!(context instanceof OrExpression)) {
                return false;
            }
            OrExpression orExpression = ((OrExpression) context);
            return expression.getLeftExpression().accept(this, orExpression.getLeftExpression()) && expression.getRightExpression().accept(
                    this, orExpression.getRightExpression());
        }

        @Override
        public Boolean visitNotExpression(NotExpression<String> expression, TupleExpression<ColumnHandle> context)
        {
            if (!(context instanceof NotExpression)) {
                return false;
            }
            return expression.getExpression().accept(this, ((NotExpression) context).getExpression());
        }

        @Override
        public Boolean visitAllExpression(AllExpression<String> expression, TupleExpression<ColumnHandle> context)
        {
            return context instanceof AllExpression;
        }

        @Override
        public Boolean visitNoneExpression(NoneExpression<String> expression, TupleExpression<ColumnHandle> context)
        {
            return context instanceof NoneExpression;
        }
    }
}
