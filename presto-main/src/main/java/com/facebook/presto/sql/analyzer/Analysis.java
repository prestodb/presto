package com.facebook.presto.sql.analyzer;

import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Table;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

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

    private final IdentityHashMap<Table, TableHandle> tables = new IdentityHashMap<>();

    private final IdentityHashMap<Expression, Type> types = new IdentityHashMap<>();
    private final IdentityHashMap<FunctionCall, FunctionInfo> functionInfo = new IdentityHashMap<>();

    private final IdentityHashMap<Field, ColumnHandle> columns = new IdentityHashMap<>();

    // for materialized views
    private QualifiedTableName destination;
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

    public void setDestination(QualifiedTableName destination)
    {
        this.destination = destination;
    }

    public QualifiedTableName getDestination()
    {
        return destination;
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
        Preconditions.checkNotNull(tableReference, "tableReference is null");
        Preconditions.checkNotNull(query, "query is null");

        namedQueries.put(tableReference, query);
    }
}

