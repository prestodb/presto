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
package com.facebook.presto.druid;

import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.LimitNode;
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
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.druid.DruidColumnHandle.DruidColumnType.REGULAR;
import static com.facebook.presto.druid.DruidQueryGeneratorContext.Origin.DERIVED;
import static com.facebook.presto.druid.DruidQueryGeneratorContext.Origin.TABLE_COLUMN;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.spi.plan.LimitNode.Step.FINAL;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public class TestDruidQueryBase
{
    protected static final FunctionAndTypeManager functionAndTypeManager = createTestFunctionAndTypeManager();
    protected static final StandardFunctionResolution standardFunctionResolution = new FunctionResolution(functionAndTypeManager);

    protected static ConnectorId druidConnectorId = new ConnectorId("id");
    protected static DruidTableHandle realtimeOnlyTable = new DruidTableHandle("schema", "realtimeOnly", Optional.empty());
    protected static DruidTableHandle hybridTable = new DruidTableHandle("schema", "hybrid", Optional.empty());
    protected static DruidColumnHandle regionId = new DruidColumnHandle("region.Id", BIGINT, REGULAR);
    protected static DruidColumnHandle city = new DruidColumnHandle("city", VARCHAR, REGULAR);
    protected static final DruidColumnHandle fare = new DruidColumnHandle("fare", DOUBLE, REGULAR);
    protected static final DruidColumnHandle secondsSinceEpoch = new DruidColumnHandle("secondsSinceEpoch", BIGINT, REGULAR);
    protected static final DruidColumnHandle datetime = new DruidColumnHandle("datetime", TIMESTAMP, REGULAR);

    protected static final Metadata metadata = MetadataManager.createTestMetadataManager();

    protected final DruidConfig druidConfig = new DruidConfig();

    protected static final Map<VariableReferenceExpression, DruidQueryGeneratorContext.Selection> testInput =
            new ImmutableMap.Builder<VariableReferenceExpression, DruidQueryGeneratorContext.Selection>()
                    .put(new VariableReferenceExpression("region.id", BIGINT), new DruidQueryGeneratorContext.Selection("region.Id", TABLE_COLUMN))
                    .put(new VariableReferenceExpression("city", VARCHAR), new DruidQueryGeneratorContext.Selection("city", TABLE_COLUMN))
                    .put(new VariableReferenceExpression("fare", DOUBLE), new DruidQueryGeneratorContext.Selection("fare", TABLE_COLUMN))
                    .put(new VariableReferenceExpression("totalfare", DOUBLE), new DruidQueryGeneratorContext.Selection("(fare + trip)", DERIVED))
                    .put(new VariableReferenceExpression("secondssinceepoch", BIGINT), new DruidQueryGeneratorContext.Selection("secondsSinceEpoch", TABLE_COLUMN))
                    .put(new VariableReferenceExpression("datetime", TIMESTAMP), new DruidQueryGeneratorContext.Selection("datetime", TABLE_COLUMN))
                    .build();

    protected final TypeProvider typeProvider = TypeProvider.fromVariables(testInput.keySet());

    protected static class SessionHolder
    {
        private final ConnectorSession connectorSession;
        private final Session session;

        public SessionHolder()
        {
            connectorSession = SESSION;
            session = TestingSession.testSessionBuilder(new SessionPropertyManager(new SystemSessionProperties().getSessionProperties())).build();
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

    protected TableScanNode tableScan(PlanBuilder planBuilder, DruidTableHandle connectorTableHandle, DruidColumnHandle... columnHandles)
    {
        List<VariableReferenceExpression> variables = Arrays.stream(columnHandles).map(ch -> new VariableReferenceExpression(ch.getColumnName().toLowerCase(ENGLISH), ch.getColumnType())).collect(toImmutableList());
        ImmutableMap.Builder<VariableReferenceExpression, ColumnHandle> assignments = ImmutableMap.builder();
        for (int i = 0; i < variables.size(); ++i) {
            assignments.put(variables.get(i), columnHandles[i]);
        }
        TableHandle tableHandle = new TableHandle(
                druidConnectorId,
                connectorTableHandle,
                TestingTransactionHandle.create(),
                Optional.empty());
        return new TableScanNode(
                planBuilder.getIdAllocator().getNextId(),
                tableHandle,
                variables,
                assignments.build(),
                TupleDomain.all(),
                TupleDomain.all());
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
}
