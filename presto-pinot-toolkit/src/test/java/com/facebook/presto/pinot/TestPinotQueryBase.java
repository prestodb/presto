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
package com.facebook.presto.pinot;

import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.pinot.query.PinotQueryGeneratorContext;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.DistinctLimitNode;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.MarkDistinctNode;
import com.facebook.presto.spi.plan.Ordering;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.ExpressionUtils;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.SqlToRowExpressionTranslator;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.testing.TestingConnectorSession;
import com.facebook.presto.testing.TestingSession;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.pinot.PinotColumnHandle.PinotColumnType.REGULAR;
import static com.facebook.presto.pinot.query.PinotQueryGeneratorContext.Origin.DERIVED;
import static com.facebook.presto.pinot.query.PinotQueryGeneratorContext.Origin.TABLE_COLUMN;
import static com.facebook.presto.spi.plan.LimitNode.Step.FINAL;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public class TestPinotQueryBase
{
    protected static final FunctionAndTypeManager functionAndTypeManager = createTestFunctionAndTypeManager();
    protected static final StandardFunctionResolution standardFunctionResolution = new FunctionResolution(functionAndTypeManager);

    protected static ConnectorId pinotConnectorId = new ConnectorId("id");
    protected static PinotTableHandle realtimeOnlyTable = new PinotTableHandle(pinotConnectorId.getCatalogName(), "schema", "realtimeOnly");
    protected static PinotTableHandle hybridTable = new PinotTableHandle(pinotConnectorId.getCatalogName(), "schema", "hybrid");
    protected static PinotColumnHandle regionId = new PinotColumnHandle("regionId", BIGINT, REGULAR);
    protected static PinotColumnHandle city = new PinotColumnHandle("city", VARCHAR, REGULAR);
    protected static final PinotColumnHandle fare = new PinotColumnHandle("fare", DOUBLE, REGULAR);
    protected static final PinotColumnHandle scores = array(DOUBLE, "scores");
    protected static final PinotColumnHandle secondsSinceEpoch = new PinotColumnHandle("secondsSinceEpoch", BIGINT, REGULAR);
    protected static final PinotColumnHandle daysSinceEpoch = new PinotColumnHandle("daysSinceEpoch", DATE, REGULAR);
    protected static final PinotColumnHandle millisSinceEpoch = new PinotColumnHandle("millisSinceEpoch", TIMESTAMP, REGULAR);

    protected static final Metadata metadata = MetadataManager.createTestMetadataManager();

    protected final PinotConfig pinotConfig = new PinotConfig();

    protected static final Map<VariableReferenceExpression, PinotQueryGeneratorContext.Selection> testInput =
            ImmutableMap.<VariableReferenceExpression, PinotQueryGeneratorContext.Selection>builder()
                    .put(new VariableReferenceExpression("regionid", BIGINT), new PinotQueryGeneratorContext.Selection("regionId", TABLE_COLUMN)) // direct column reference
                    .put(new VariableReferenceExpression("regionid_33", BIGINT), new PinotQueryGeneratorContext.Selection("regionId", TABLE_COLUMN)) // direct column reference
                    .put(new VariableReferenceExpression("regionid$distinct", BIGINT), new PinotQueryGeneratorContext.Selection("regionId", TABLE_COLUMN)) // distinct column reference
                    .put(new VariableReferenceExpression("regionid$distinct_62", BIGINT), new PinotQueryGeneratorContext.Selection("regionId", TABLE_COLUMN)) // distinct column reference
                    .put(new VariableReferenceExpression("city", VARCHAR), new PinotQueryGeneratorContext.Selection("city", TABLE_COLUMN)) // direct column reference
                    .put(new VariableReferenceExpression("scores", new ArrayType(DOUBLE)), new PinotQueryGeneratorContext.Selection("scores", TABLE_COLUMN)) // direct column reference
                    .put(new VariableReferenceExpression("fare", DOUBLE), new PinotQueryGeneratorContext.Selection("fare", TABLE_COLUMN)) // direct column reference
                    .put(new VariableReferenceExpression("totalfare", DOUBLE), new PinotQueryGeneratorContext.Selection("(fare + trip)", DERIVED)) // derived column
                    .put(new VariableReferenceExpression("count_regionid", BIGINT), new PinotQueryGeneratorContext.Selection("count(regionid)", DERIVED))// derived column
                    .put(new VariableReferenceExpression("sum_fare", BIGINT), new PinotQueryGeneratorContext.Selection("sum(fare)", DERIVED))// derived column
                    .put(new VariableReferenceExpression("array_min_0", DOUBLE), new PinotQueryGeneratorContext.Selection("array_min(scores)", DERIVED)) // derived column
                    .put(new VariableReferenceExpression("array_max_0", DOUBLE), new PinotQueryGeneratorContext.Selection("array_max(scores)", DERIVED)) // derived column
                    .put(new VariableReferenceExpression("array_sum_0", DOUBLE), new PinotQueryGeneratorContext.Selection("reduce(scores, cast(0 as double), (s, x) -> s + x, s -> s)", DERIVED)) // derived column
                    .put(new VariableReferenceExpression("array_average_0", DOUBLE), new PinotQueryGeneratorContext.Selection("reduce(scores, CAST(ROW(0.0, 0) AS ROW(sum DOUBLE, count INTEGER)), (s,x) -> CAST(ROW(x + s.sum, s.count + 1) AS ROW(sum DOUBLE, count INTEGER)), s -> IF(s.count = 0, NULL, s.sum / s.count))", DERIVED)) // derived column
                    .put(new VariableReferenceExpression("secondssinceepoch", BIGINT), new PinotQueryGeneratorContext.Selection("secondsSinceEpoch", TABLE_COLUMN)) // column for datetime functions
                    .put(new VariableReferenceExpression("dayssinceepoch", DATE), new PinotQueryGeneratorContext.Selection("daysSinceEpoch", TABLE_COLUMN)) // column for date functions
                    .put(new VariableReferenceExpression("millissinceepoch", TIMESTAMP), new PinotQueryGeneratorContext.Selection("millisSinceEpoch", TABLE_COLUMN)) // column for timestamp functions
                    .build();

    protected final TypeProvider typeProvider = TypeProvider.fromVariables(testInput.keySet());

    protected static class SessionHolder
    {
        private final ConnectorSession connectorSession;
        private final Session session;

        public SessionHolder(PinotConfig pinotConfig)
        {
            connectorSession = new TestingConnectorSession(new PinotSessionProperties(pinotConfig).getSessionProperties());
            session = TestingSession.testSessionBuilder(new SessionPropertyManager(new SystemSessionProperties().getSessionProperties())).build();
        }

        public SessionHolder(boolean useDateTrunc, boolean useSqlSyntax)
        {
            this(new PinotConfig().setUseDateTrunc(useDateTrunc).setUsePinotSqlForBrokerQueries(useSqlSyntax));
        }

        public ConnectorSession getConnectorSession()
        {
            return connectorSession;
        }

        public Session getSession()
        {
            return session;
        }
    }

    protected VariableReferenceExpression variable(String name)
    {
        return testInput.keySet().stream().filter(v -> v.getName().equals(name)).findFirst().orElseThrow(() -> new IllegalArgumentException("Cannot find variable " + name));
    }

    protected TableScanNode tableScan(PlanBuilder planBuilder, PinotTableHandle connectorTableHandle, PinotColumnHandle... columnHandles)
    {
        Map<VariableReferenceExpression, PinotColumnHandle> columnHandleMap = new LinkedHashMap<>();
        Arrays.stream(columnHandles).forEachOrdered(ch -> columnHandleMap.put(new VariableReferenceExpression(ch.getColumnName().toLowerCase(ENGLISH), ch.getDataType()), ch));
        return tableScan(planBuilder, connectorTableHandle, columnHandleMap);
    }

    protected TableScanNode tableScan(PlanBuilder planBuilder, PinotTableHandle connectorTableHandle, Map<VariableReferenceExpression, PinotColumnHandle> columnHandles)
    {
        List<VariableReferenceExpression> variables = ImmutableList.copyOf(columnHandles.keySet());
        ImmutableMap.Builder<VariableReferenceExpression, ColumnHandle> assignments = ImmutableMap.builder();
        for (VariableReferenceExpression variable : columnHandles.keySet()) {
            assignments.put(variable, columnHandles.get(variable));
        }
        TableHandle tableHandle = new TableHandle(
                pinotConnectorId,
                connectorTableHandle,
                TestingTransactionHandle.create(),
                Optional.empty());
        return planBuilder.tableScan(
                tableHandle,
                variables,
                assignments.build());
    }

    protected MarkDistinctNode markDistinct(PlanBuilder planBuilder, VariableReferenceExpression markerVariable, List<VariableReferenceExpression> distinctVariables, PlanNode source)
    {
        return planBuilder.markDistinct(markerVariable, distinctVariables, source);
    }

    protected FilterNode filter(PlanBuilder planBuilder, PlanNode source, RowExpression predicate)
    {
        return planBuilder.filter(predicate, source);
    }

    protected ProjectNode project(PlanBuilder planBuilder, PlanNode source, List<String> columnNames)
    {
        Map<String, VariableReferenceExpression> incomingColumns = source.getOutputVariables().stream().collect(toMap(VariableReferenceExpression::getName, identity()));
        Assignments.Builder assignmentsBuilder = Assignments.builder();
        columnNames.forEach(columnName -> {
            VariableReferenceExpression variable = requireNonNull(incomingColumns.get(columnName), "Couldn't find the incoming column " + columnName);
            assignmentsBuilder.put(variable, variable);
        });
        return planBuilder.project(assignmentsBuilder.build(), source);
    }

    protected ProjectNode project(PlanBuilder planBuilder, PlanNode source, LinkedHashMap<String, String> toProject, SessionHolder sessionHolder)
    {
        Assignments.Builder assignmentsBuilder = Assignments.builder();
        toProject.forEach((columnName, expression) -> {
            RowExpression rowExpression = getRowExpression(expression, sessionHolder);
            VariableReferenceExpression variable = new VariableReferenceExpression(columnName, rowExpression.getType());
            assignmentsBuilder.put(variable, rowExpression);
        });
        return planBuilder.project(assignmentsBuilder.build(), source);
    }

    public static Expression expression(String sql)
    {
        return ExpressionUtils.rewriteIdentifiersToSymbolReferences(new SqlParser().createExpression(sql, new ParsingOptions(ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL)));
    }

    protected RowExpression toRowExpression(Expression expression, Session session)
    {
        Map<NodeRef<Expression>, Type> expressionTypes = getExpressionTypes(
                session,
                metadata,
                new SqlParser(),
                typeProvider,
                expression,
                ImmutableList.of(),
                WarningCollector.NOOP);
        return SqlToRowExpressionTranslator.translate(expression, expressionTypes, ImmutableMap.of(), functionAndTypeManager, session);
    }

    protected LimitNode limit(PlanBuilder pb, long count, PlanNode source)
    {
        return new LimitNode(pb.getIdAllocator().getNextId(), source, count, FINAL);
    }

    protected DistinctLimitNode distinctLimit(PlanBuilder pb, List<VariableReferenceExpression> distinctVariables, long count, PlanNode source)
    {
        return new DistinctLimitNode(pb.getIdAllocator().getNextId(), source, count, false, distinctVariables, Optional.empty());
    }

    protected TopNNode topN(PlanBuilder pb, long count, List<String> orderingColumns, List<Boolean> ascending, PlanNode source)
    {
        ImmutableList<Ordering> ordering = IntStream.range(0, orderingColumns.size()).boxed().map(i -> new Ordering(variable(orderingColumns.get(i)), ascending.get(i) ? SortOrder.ASC_NULLS_FIRST : SortOrder.DESC_NULLS_FIRST)).collect(toImmutableList());
        return new TopNNode(pb.getIdAllocator().getNextId(), source, count, new OrderingScheme(ordering), TopNNode.Step.SINGLE);
    }

    protected RowExpression getRowExpression(String sqlExpression, SessionHolder sessionHolder)
    {
        return toRowExpression(expression(sqlExpression), sessionHolder.getSession());
    }

    protected PlanBuilder createPlanBuilder(SessionHolder sessionHolder)
    {
        return new PlanBuilder(sessionHolder.getSession(), new PlanNodeIdAllocator(), metadata);
    }

    protected static PinotColumnHandle derived(String name)
    {
        return new PinotColumnHandle(name, BIGINT, PinotColumnHandle.PinotColumnType.DERIVED);
    }

    protected static PinotColumnHandle integer(String name)
    {
        return new PinotColumnHandle(name, INTEGER, PinotColumnHandle.PinotColumnType.REGULAR);
    }

    protected static PinotColumnHandle bigint(String name)
    {
        return new PinotColumnHandle(name, BIGINT, PinotColumnHandle.PinotColumnType.REGULAR);
    }

    protected static PinotColumnHandle fraction(String name)
    {
        return new PinotColumnHandle(name, DOUBLE, PinotColumnHandle.PinotColumnType.REGULAR);
    }

    protected static PinotColumnHandle varchar(String name)
    {
        return new PinotColumnHandle(name, VARCHAR, PinotColumnHandle.PinotColumnType.REGULAR);
    }

    protected static PinotColumnHandle array(Type type, String name)
    {
        return new PinotColumnHandle(name, new ArrayType(type), PinotColumnHandle.PinotColumnType.REGULAR);
    }
}
