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

import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.SampledRelation;
import com.facebook.presto.sql.tree.Table;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.SetMultimap;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class Analysis
{
    private Query query;

    private final IdentityHashMap<Table, Query> namedQueries = new IdentityHashMap<>();

    private TupleDescriptor outputDescriptor;
    private final IdentityHashMap<Node, TupleDescriptor> outputDescriptors = new IdentityHashMap<>();
    private final IdentityHashMap<Expression, Map<QualifiedName, Integer>> resolvedNames = new IdentityHashMap<>();

    private final IdentityHashMap<QuerySpecification, List<FunctionCall>> aggregates = new IdentityHashMap<>();
    private final IdentityHashMap<QuerySpecification, List<FieldOrExpression>> groupByExpressions = new IdentityHashMap<>();
    private final IdentityHashMap<QuerySpecification, Expression> where = new IdentityHashMap<>();
    private final IdentityHashMap<QuerySpecification, Expression> having = new IdentityHashMap<>();
    private final IdentityHashMap<Node, List<FieldOrExpression>> orderByExpressions = new IdentityHashMap<>();
    private final IdentityHashMap<Node, List<FieldOrExpression>> outputExpressions = new IdentityHashMap<>();
    private final IdentityHashMap<QuerySpecification, List<FunctionCall>> windowFunctions = new IdentityHashMap<>();

    private final IdentityHashMap<Join, List<EquiJoinClause>> joins = new IdentityHashMap<>();
    private final SetMultimap<Node, InPredicate> inPredicates = HashMultimap.create();
    private final IdentityHashMap<Join, JoinInPredicates> joinInPredicates = new IdentityHashMap<>();

    private final IdentityHashMap<Table, TableHandle> tables = new IdentityHashMap<>();

    private final IdentityHashMap<Expression, Type> types = new IdentityHashMap<>();
    private final IdentityHashMap<FunctionCall, FunctionInfo> functionInfo = new IdentityHashMap<>();

    private final IdentityHashMap<Field, ColumnHandle> columns = new IdentityHashMap<>();

    private final IdentityHashMap<SampledRelation, Double> sampleRatios = new IdentityHashMap<>();

    // for create table
    private Optional<QualifiedTableName> createTableDestination = Optional.absent();

    // for materialized views
    private Optional<QualifiedTableName> materializedViewDestination = Optional.absent();
    private Optional<Integer> refreshInterval;
    private boolean refresh;

    public Query getQuery()
    {
        return query;
    }

    public void setQuery(Query query)
    {
        this.query = query;
    }

    public void addResolvedNames(Expression expression, Map<QualifiedName, Integer> mappings)
    {
        resolvedNames.put(expression, mappings);
    }

    public Map<QualifiedName, Integer> getResolvedNames(Expression expression)
    {
        return resolvedNames.get(expression);
    }

    public void setAggregates(QuerySpecification node, List<FunctionCall> aggregates)
    {
        this.aggregates.put(node, aggregates);
    }

    public List<FunctionCall> getAggregates(QuerySpecification query)
    {
        return aggregates.get(query);
    }

    public Type getType(Expression expression)
    {
        Preconditions.checkArgument(types.containsKey(expression), "Expression not analyzed: %s", expression);
        return types.get(expression);
    }

    public void setGroupByExpressions(QuerySpecification node, List<FieldOrExpression> expressions)
    {
        groupByExpressions.put(node, expressions);
    }

    public List<FieldOrExpression> getGroupByExpressions(QuerySpecification node)
    {
        return groupByExpressions.get(node);
    }

    public void setWhere(QuerySpecification node, Expression expression)
    {
        where.put(node, expression);
    }

    public Expression getWhere(QuerySpecification node)
    {
        return where.get(node);
    }

    public void setOrderByExpressions(Node node, List<FieldOrExpression> items)
    {
        orderByExpressions.put(node, items);
    }

    public List<FieldOrExpression> getOrderByExpressions(Node node)
    {
        return orderByExpressions.get(node);
    }

    public void setOutputExpressions(Node node, List<FieldOrExpression> expressions)
    {
        outputExpressions.put(node, expressions);
    }

    public List<FieldOrExpression> getOutputExpressions(Node node)
    {
        return outputExpressions.get(node);
    }

    public void setHaving(QuerySpecification node, Expression expression)
    {
        having.put(node, expression);
    }

    public void setEquijoinCriteria(Join node, List<EquiJoinClause> clauses)
    {
        joins.put(node, clauses);
    }

    public List<EquiJoinClause> getJoinCriteria(Join join)
    {
        return joins.get(join);
    }

    public void addInPredicates(Query node, Set<InPredicate> inPredicates)
    {
        this.inPredicates.putAll(node, inPredicates);
    }

    public void addInPredicates(QuerySpecification node, Set<InPredicate> inPredicates)
    {
        this.inPredicates.putAll(node, inPredicates);
    }

    public Set<InPredicate> getInPredicates(Query node)
    {
        return inPredicates.get(node);
    }

    public Set<InPredicate> getInPredicates(QuerySpecification node)
    {
        return inPredicates.get(node);
    }

    public void addJoinInPredicates(Join node, JoinInPredicates joinInPredicates)
    {
        this.joinInPredicates.put(node, joinInPredicates);
    }

    public JoinInPredicates getJoinInPredicates(Join node)
    {
        return joinInPredicates.get(node);
    }

    public void setWindowFunctions(QuerySpecification node, List<FunctionCall> functions)
    {
        windowFunctions.put(node, functions);
    }

    public Map<QuerySpecification, List<FunctionCall>> getWindowFunctions()
    {
        return windowFunctions;
    }

    public List<FunctionCall> getWindowFunctions(QuerySpecification query)
    {
        return windowFunctions.get(query);
    }

    public void setOutputDescriptor(TupleDescriptor descriptor)
    {
        outputDescriptor = descriptor;
    }

    public TupleDescriptor getOutputDescriptor()
    {
        return outputDescriptor;
    }

    public void setOutputDescriptor(Node node, TupleDescriptor descriptor)
    {
        outputDescriptors.put(node, descriptor);
    }

    public TupleDescriptor getOutputDescriptor(Node node)
    {
        Preconditions.checkState(outputDescriptors.containsKey(node), "Output descriptor missing for %s. Broken analysis?", node);
        return outputDescriptors.get(node);
    }

    public TableHandle getTableHandle(Table table)
    {
        return tables.get(table);
    }

    public void registerTable(Table table, TableHandle handle)
    {
        tables.put(table, handle);
    }

    public FunctionInfo getFunctionInfo(FunctionCall function)
    {
        return functionInfo.get(function);
    }

    public void addFunctionInfos(IdentityHashMap<FunctionCall, FunctionInfo> infos)
    {
        functionInfo.putAll(infos);
    }

    public void addTypes(IdentityHashMap<Expression, Type> types)
    {
        this.types.putAll(types);
    }

    public Expression getHaving(QuerySpecification query)
    {
        return having.get(query);
    }

    public void setColumn(Field field, ColumnHandle handle)
    {
        columns.put(field, handle);
    }

    public ColumnHandle getColumn(Field field)
    {
        return columns.get(field);
    }

    public void setCreateTableDestination(QualifiedTableName destination)
    {
        this.createTableDestination = Optional.of(destination);
    }

    public Optional<QualifiedTableName> getCreateTableDestination()
    {
        return createTableDestination;
    }

    public void setMaterializedViewDestination(QualifiedTableName destination)
    {
        this.materializedViewDestination = Optional.of(destination);
    }

    public Optional<QualifiedTableName> getMaterializedViewDestination()
    {
        return materializedViewDestination;
    }

    public Optional<Integer> getRefreshInterval()
    {
        return refreshInterval;
    }

    public void setDoRefresh(boolean refresh)
    {
        this.refresh = refresh;
    }

    public boolean isDoRefresh()
    {
        return refresh;
    }

    public void setRefreshInterval(Optional<Integer> refreshInterval)
    {
        this.refreshInterval = refreshInterval;
    }

    public Query getNamedQuery(Table table)
    {
        return namedQueries.get(table);
    }

    public void registerNamedQuery(Table tableReference, Query query)
    {
        checkNotNull(tableReference, "tableReference is null");
        checkNotNull(query, "query is null");

        namedQueries.put(tableReference, query);
    }

    public void setSampleRatio(SampledRelation relation, double ratio)
    {
        sampleRatios.put(relation, ratio);
    }

    public double getSampleRatio(SampledRelation relation)
    {
        Preconditions.checkState(sampleRatios.containsKey(relation), "Sample ratio missing for %s. Broken analysis?", relation);
        return sampleRatios.get(relation);
    }

    public static class JoinInPredicates
    {
        private final Set<InPredicate> leftInPredicates;
        private final Set<InPredicate> rightInPredicates;

        public JoinInPredicates(Set<InPredicate> leftInPredicates, Set<InPredicate> rightInPredicates)
        {
            this.leftInPredicates = ImmutableSet.copyOf(checkNotNull(leftInPredicates, "leftInPredicates is null"));
            this.rightInPredicates = ImmutableSet.copyOf(checkNotNull(rightInPredicates, "rightInPredicates is null"));
        }

        public Set<InPredicate> getLeftInPredicates()
        {
            return leftInPredicates;
        }

        public Set<InPredicate> getRightInPredicates()
        {
            return rightInPredicates;
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(leftInPredicates, rightInPredicates);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            final JoinInPredicates other = (JoinInPredicates) obj;
            return Objects.equal(this.leftInPredicates, other.leftInPredicates) &&
                    Objects.equal(this.rightInPredicates, other.rightInPredicates);
        }
    }
}

