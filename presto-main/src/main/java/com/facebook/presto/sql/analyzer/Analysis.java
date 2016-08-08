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

import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.ExistsPredicate;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.SampledRelation;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.SubqueryExpression;
import com.facebook.presto.sql.tree.Table;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Sets.newIdentityHashSet;
import static java.util.Objects.requireNonNull;

public class Analysis
{
    private final Statement root;
    private String updateType;

    private final IdentityHashMap<Table, Query> namedQueries = new IdentityHashMap<>();

    private final IdentityHashMap<Node, Scope> scopes = new IdentityHashMap<>();
    private final Set<Expression> columnReferences = newIdentityHashSet();

    private final IdentityHashMap<QuerySpecification, List<FunctionCall>> aggregates = new IdentityHashMap<>();
    private final IdentityHashMap<QuerySpecification, List<List<Expression>>> groupByExpressions = new IdentityHashMap<>();
    private final IdentityHashMap<Node, Expression> where = new IdentityHashMap<>();
    private final IdentityHashMap<QuerySpecification, Expression> having = new IdentityHashMap<>();
    private final IdentityHashMap<Node, List<Expression>> orderByExpressions = new IdentityHashMap<>();
    private final IdentityHashMap<Node, List<Expression>> outputExpressions = new IdentityHashMap<>();
    private final IdentityHashMap<QuerySpecification, List<FunctionCall>> windowFunctions = new IdentityHashMap<>();

    private final IdentityHashMap<Join, Expression> joins = new IdentityHashMap<>();
    private final ListMultimap<Node, InPredicate> inPredicatesSubqueries = ArrayListMultimap.create();
    private final ListMultimap<Node, SubqueryExpression> scalarSubqueries = ArrayListMultimap.create();
    private final ListMultimap<Node, ExistsPredicate> existsSubqueries = ArrayListMultimap.create();

    private final IdentityHashMap<Table, TableHandle> tables = new IdentityHashMap<>();

    private final IdentityHashMap<Expression, Type> types = new IdentityHashMap<>();
    private final IdentityHashMap<Expression, Type> coercions = new IdentityHashMap<>();
    private final Set<Expression> typeOnlyCoercions = newIdentityHashSet();
    private final IdentityHashMap<Relation, Type[]> relationCoercions = new IdentityHashMap<>();
    private final IdentityHashMap<FunctionCall, Signature> functionSignature = new IdentityHashMap<>();

    private final IdentityHashMap<Field, ColumnHandle> columns = new IdentityHashMap<>();

    private final IdentityHashMap<SampledRelation, Double> sampleRatios = new IdentityHashMap<>();

    // for create table
    private Optional<QualifiedObjectName> createTableDestination = Optional.empty();
    private Map<String, Expression> createTableProperties = ImmutableMap.of();
    private boolean createTableAsSelectWithData = true;
    private boolean createTableAsSelectNoOp = false;

    private Optional<Insert> insert = Optional.empty();

    public Analysis(Statement root)
    {
        this.root = root;
    }

    public Statement getStatement()
    {
        return root;
    }

    public String getUpdateType()
    {
        return updateType;
    }

    public void setUpdateType(String updateType)
    {
        this.updateType = updateType;
    }

    public boolean isCreateTableAsSelectWithData()
    {
        return createTableAsSelectWithData;
    }

    public void setCreateTableAsSelectWithData(boolean createTableAsSelectWithData)
    {
        this.createTableAsSelectWithData = createTableAsSelectWithData;
    }

    public boolean isCreateTableAsSelectNoOp()
    {
        return createTableAsSelectNoOp;
    }

    public void setCreateTableAsSelectNoOp(boolean createTableAsSelectNoOp)
    {
        this.createTableAsSelectNoOp = createTableAsSelectNoOp;
    }

    public void setAggregates(QuerySpecification node, List<FunctionCall> aggregates)
    {
        this.aggregates.put(node, aggregates);
    }

    public List<FunctionCall> getAggregates(QuerySpecification query)
    {
        return aggregates.get(query);
    }

    public IdentityHashMap<Expression, Type> getTypes()
    {
        return new IdentityHashMap<>(types);
    }

    public Type getType(Expression expression)
    {
        checkArgument(types.containsKey(expression), "Expression not analyzed: %s", expression);
        return types.get(expression);
    }

    public Type getTypeWithCoercions(Expression expression)
    {
        checkArgument(types.containsKey(expression), "Expression not analyzed: %s", expression);
        if (coercions.containsKey(expression)) {
            return coercions.get(expression);
        }
        return types.get(expression);
    }

    public Type[] getRelationCoercion(Relation relation)
    {
        return relationCoercions.get(relation);
    }

    public void addRelationCoercion(Relation relation, Type[] types)
    {
        relationCoercions.put(relation, types);
    }

    public IdentityHashMap<Expression, Type> getCoercions()
    {
        return coercions;
    }

    public Type getCoercion(Expression expression)
    {
        return coercions.get(expression);
    }

    public void setGroupingSets(QuerySpecification node, List<List<Expression>> expressions)
    {
        groupByExpressions.put(node, expressions);
    }

    public boolean isTypeOnlyCoercion(Expression expression)
    {
        return typeOnlyCoercions.contains(expression);
    }

    public List<List<Expression>> getGroupingSets(QuerySpecification node)
    {
        return groupByExpressions.get(node);
    }

    public void setWhere(Node node, Expression expression)
    {
        where.put(node, expression);
    }

    public Expression getWhere(QuerySpecification node)
    {
        return where.get(node);
    }

    public void setOrderByExpressions(Node node, List<Expression> items)
    {
        orderByExpressions.put(node, items);
    }

    public List<Expression> getOrderByExpressions(Node node)
    {
        return orderByExpressions.get(node);
    }

    public void setOutputExpressions(Node node, List<Expression> expressions)
    {
        outputExpressions.put(node, expressions);
    }

    public List<Expression> getOutputExpressions(Node node)
    {
        return outputExpressions.get(node);
    }

    public void setHaving(QuerySpecification node, Expression expression)
    {
        having.put(node, expression);
    }

    public void setJoinCriteria(Join node, Expression criteria)
    {
        joins.put(node, criteria);
    }

    public Expression getJoinCriteria(Join join)
    {
        return joins.get(join);
    }

    public void recordSubqueries(Node node, ExpressionAnalysis expressionAnalysis)
    {
        this.inPredicatesSubqueries.putAll(node, expressionAnalysis.getSubqueryInPredicates());
        this.scalarSubqueries.putAll(node, expressionAnalysis.getScalarSubqueries());
        this.existsSubqueries.putAll(node, expressionAnalysis.getExistsSubqueries());
    }

    public List<InPredicate> getInPredicateSubqueries(Node node)
    {
        if (inPredicatesSubqueries.containsKey(node)) {
            return inPredicatesSubqueries.get(node);
        }
        return ImmutableList.of();
    }

    public List<SubqueryExpression> getScalarSubqueries(Node node)
    {
        if (scalarSubqueries.containsKey(node)) {
            return scalarSubqueries.get(node);
        }
        return ImmutableList.of();
    }

    public List<ExistsPredicate> getExistsSubqueries(Node node)
    {
        if (existsSubqueries.containsKey(node)) {
            return existsSubqueries.get(node);
        }
        return ImmutableList.of();
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

    public void addColumnReferences(Set<Expression> columnReferences)
    {
        this.columnReferences.addAll(columnReferences);
    }

    public Scope getScope(Node node)
    {
        return tryGetScope(node).orElseThrow(() -> new IllegalArgumentException(String.format("Analysis does not contain information for node: %s", node)));
    }

    public Optional<Scope> tryGetScope(Node node)
    {
        if (scopes.containsKey(node)) {
            return Optional.of(scopes.get(node));
        }

        if (root == null) {
            return Optional.empty();
        }

        GetScopeVisitor visitor = new GetScopeVisitor(scopes, node);
        visitor.process(root, null);
        return visitor.getResult();
    }

    public Scope getRootScope()
    {
        return getScope(root);
    }

    private static class GetScopeVisitor
            extends DefaultTraversalVisitor<Void, Scope>
    {
        private final IdentityHashMap<Node, Scope> scopes;
        private final Node node;
        private Scope result;

        public GetScopeVisitor(IdentityHashMap<Node, Scope> scopes, Node node)
        {
            this.scopes = requireNonNull(scopes, "scopes is null");
            this.node = requireNonNull(node, "node is null");
        }

        @Override
        public Void process(Node current, @Nullable Scope candidate)
        {
            if (result != null) {
                return null;
            }

            if (scopes.containsKey(current)) {
                candidate = scopes.get(current);
            }
            if (node == current) {
                result = candidate;
            }
            else {
                super.process(current, candidate);
            }

            return null;
        }

        public Optional<Scope> getResult()
        {
            return Optional.ofNullable(result);
        }
    }

    public void setScope(Node node, Scope scope)
    {
        scopes.put(node, scope);
    }

    public RelationType getOutputDescriptor()
    {
        return getOutputDescriptor(root);
    }

    public RelationType getOutputDescriptor(Node node)
    {
        return getScope(node).getRelationType();
    }

    public TableHandle getTableHandle(Table table)
    {
        return tables.get(table);
    }

    public void registerTable(Table table, TableHandle handle)
    {
        tables.put(table, handle);
    }

    public Signature getFunctionSignature(FunctionCall function)
    {
        return functionSignature.get(function);
    }

    public void addFunctionSignatures(IdentityHashMap<FunctionCall, Signature> infos)
    {
        functionSignature.putAll(infos);
    }

    public Set<Expression> getColumnReferences()
    {
        return ImmutableSet.copyOf(columnReferences);
    }

    public void addTypes(IdentityHashMap<Expression, Type> types)
    {
        this.types.putAll(types);
    }

    public void addCoercion(Expression expression, Type type, boolean isTypeOnlyCoercion)
    {
        this.coercions.put(expression, type);
        if (isTypeOnlyCoercion) {
            this.typeOnlyCoercions.add(expression);
        }
    }

    public void addCoercions(IdentityHashMap<Expression, Type> coercions, Set<Expression> typeOnlyCoercions)
    {
        this.coercions.putAll(coercions);
        this.typeOnlyCoercions.addAll(typeOnlyCoercions);
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

    public void setCreateTableDestination(QualifiedObjectName destination)
    {
        this.createTableDestination = Optional.of(destination);
    }

    public Optional<QualifiedObjectName> getCreateTableDestination()
    {
        return createTableDestination;
    }

    public void setCreateTableProperties(Map<String, Expression> createTableProperties)
    {
        this.createTableProperties = createTableProperties;
    }

    public Map<String, Expression> getCreateTableProperties()
    {
        return createTableProperties;
    }

    public void setInsert(Insert insert)
    {
        this.insert = Optional.of(insert);
    }

    public Optional<Insert> getInsert()
    {
        return insert;
    }

    public Query getNamedQuery(Table table)
    {
        return namedQueries.get(table);
    }

    public void registerNamedQuery(Table tableReference, Query query)
    {
        requireNonNull(tableReference, "tableReference is null");
        requireNonNull(query, "query is null");

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

    @Immutable
    public static final class Insert
    {
        private final TableHandle target;
        private final List<ColumnHandle> columns;

        public Insert(TableHandle target, List<ColumnHandle> columns)
        {
            this.target = requireNonNull(target, "target is null");
            this.columns = requireNonNull(columns, "columns is null");
            checkArgument(columns.size() > 0, "No columns given to insert");
        }

        public List<ColumnHandle> getColumns()
        {
            return columns;
        }

        public TableHandle getTarget()
        {
            return target;
        }
    }
}
