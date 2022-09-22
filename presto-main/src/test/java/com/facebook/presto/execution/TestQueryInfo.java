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
package com.facebook.presto.execution;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.JsonModule;
import com.facebook.presto.common.plan.PlanCanonicalizationStrategy;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.HandleJsonModule;
import com.facebook.presto.server.SliceDeserializer;
import com.facebook.presto.server.SliceSerializer;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.PrestoWarning;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.WarningCode;
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.resourceGroups.QueryType;
import com.facebook.presto.spi.security.SelectedRole;
import com.facebook.presto.sql.Serialization;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.CanonicalPlan;
import com.facebook.presto.sql.planner.CanonicalPlanWithInfo;
import com.facebook.presto.sql.planner.PlanNodeCanonicalInfo;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.transaction.TransactionId;
import com.facebook.presto.type.TypeDeserializer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Optional;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.airlift.json.JsonBinder.jsonBinder;
import static com.facebook.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.execution.QueryState.FINISHED;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static org.testng.Assert.assertEquals;

public class TestQueryInfo
{
    @Test
    public void testQueryInfoRoundTrip()
    {
        JsonCodec<QueryInfo> codec = createJsonCodec();
        QueryInfo expected = createQueryInfo();
        QueryInfo actual = codec.fromJson(codec.toJson(expected));

        assertEquals(actual.getQueryId(), expected.getQueryId());
        // Note: SessionRepresentation.equals?
        assertEquals(actual.getState(), expected.getState());
        assertEquals(actual.getMemoryPool(), expected.getMemoryPool());
        assertEquals(actual.isScheduled(), expected.isScheduled());

        assertEquals(actual.getSelf(), expected.getSelf());
        assertEquals(actual.getFieldNames(), expected.getFieldNames());
        assertEquals(actual.getQuery(), expected.getQuery());
        assertEquals(actual.getQueryHash(), expected.getQueryHash());
        assertEquals(actual.getExpandedQuery(), expected.getExpandedQuery());
        assertEquals(actual.getPreparedQuery(), expected.getPreparedQuery());
        // Assert all of queryStats
        TestQueryStats.assertExpectedQueryStats(actual.getQueryStats());

        assertEquals(actual.getSetCatalog(), expected.getSetCatalog());
        assertEquals(actual.getSetSchema(), expected.getSetSchema());
        assertEquals(actual.getSetSessionProperties(), expected.getSetSessionProperties());
        assertEquals(actual.getResetSessionProperties(), expected.getResetSessionProperties());
        assertEquals(actual.getSetRoles(), expected.getSetRoles());
        assertEquals(actual.getAddedPreparedStatements(), expected.getAddedPreparedStatements());
        assertEquals(actual.getDeallocatedPreparedStatements(), expected.getDeallocatedPreparedStatements());

        assertEquals(actual.getStartedTransactionId(), expected.getStartedTransactionId());
        assertEquals(actual.isClearTransactionId(), expected.isClearTransactionId());

        assertEquals(actual.getUpdateType(), expected.getUpdateType());
        assertEquals(actual.getOutputStage(), expected.getOutputStage());

        assertEquals(actual.getFailureInfo(), expected.getFailureInfo());
        assertEquals(actual.getErrorCode(), expected.getErrorCode());
        assertEquals(actual.getWarnings(), expected.getWarnings());

        assertEquals(actual.getInputs(), expected.getInputs());
        assertEquals(actual.getOutput(), expected.getOutput());

        assertEquals(actual.isFinalQueryInfo(), expected.isFinalQueryInfo());

        assertEquals(actual.getResourceGroupId(), expected.getResourceGroupId());
        assertEquals(actual.getQueryType(), expected.getQueryType());

        assertEquals(actual.getFailedTasks(), expected.getFailedTasks());
        assertEquals(actual.getRuntimeOptimizedStages(), actual.getRuntimeOptimizedStages());

        assertEquals(actual.getAddedSessionFunctions(), expected.getAddedSessionFunctions());
        assertEquals(actual.getRemovedSessionFunctions(), expected.getRemovedSessionFunctions());
        // Test that planCanonicalInfo is not serialized
        assertEquals(actual.getPlanCanonicalInfo(), ImmutableList.of());
    }

    private static JsonCodec<QueryInfo> createJsonCodec()
    {
        Module module = binder -> {
            SqlParser sqlParser = new SqlParser();
            FunctionAndTypeManager functionAndTypeManager = createTestFunctionAndTypeManager();
            binder.install(new JsonModule());
            binder.install(new HandleJsonModule());
            binder.bind(SqlParser.class).toInstance(sqlParser);
            binder.bind(TypeManager.class).toInstance(functionAndTypeManager);
            configBinder(binder).bindConfig(FeaturesConfig.class);
            newSetBinder(binder, Type.class);
            jsonBinder(binder).addSerializerBinding(Slice.class).to(SliceSerializer.class);
            jsonBinder(binder).addDeserializerBinding(Slice.class).to(SliceDeserializer.class);
            jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
            jsonBinder(binder).addSerializerBinding(Expression.class).to(Serialization.ExpressionSerializer.class);
            jsonBinder(binder).addDeserializerBinding(Expression.class).to(Serialization.ExpressionDeserializer.class);
            jsonBinder(binder).addDeserializerBinding(FunctionCall.class).to(Serialization.FunctionCallDeserializer.class);
            jsonBinder(binder).addKeySerializerBinding(VariableReferenceExpression.class).to(Serialization.VariableReferenceExpressionSerializer.class);
            jsonBinder(binder).addKeyDeserializerBinding(VariableReferenceExpression.class).to(Serialization.VariableReferenceExpressionDeserializer.class);
            jsonCodecBinder(binder).bindJsonCodec(QueryInfo.class);
        };
        Bootstrap app = new Bootstrap(ImmutableList.of(module));
        Injector injector = app
                .doNotInitializeLogging()
                .quiet()
                .initialize();
        return injector.getInstance(new Key<JsonCodec<QueryInfo>>() {});
    }

    private static QueryInfo createQueryInfo()
    {
        return new QueryInfo(
                new QueryId("0"),
                TEST_SESSION.toSessionRepresentation(),
                FINISHED,
                new MemoryPoolId("memory_pool"),
                true,
                URI.create("1"),
                ImmutableList.of("number"),
                "SELECT 1",
                Optional.of("expanded_query"),
                Optional.of("prepared_query"),
                TestQueryStats.EXPECTED,
                Optional.of("set_catalog"),
                Optional.of("set_schema"),
                ImmutableMap.of("set_property", "set_value"),
                ImmutableSet.of("reset_property"),
                ImmutableMap.of("set_roles", new SelectedRole(SelectedRole.Type.ROLE, Optional.of("role"))),
                ImmutableMap.of("added_prepared_statement", "statement"),
                ImmutableSet.of("deallocated_prepared_statement", "statement"),
                Optional.of(TransactionId.create()),
                true,
                "update_type",
                Optional.empty(),
                null,
                null,
                ImmutableList.of(new PrestoWarning(new WarningCode(1, "name"), "message")),
                ImmutableSet.of(new Input(new ConnectorId("connector"), "schema", "table", Optional.empty(), ImmutableList.of(new Column("name", "type")), Optional.empty(), "")),
                Optional.empty(),
                true,
                Optional.empty(),
                Optional.of(QueryType.SELECT),
                Optional.of(ImmutableList.of(new TaskId("0", 1, 1, 1))),
                Optional.of(ImmutableList.of(new StageId("0", 1))),
                ImmutableMap.of(),
                ImmutableSet.of(),
                StatsAndCosts.empty(),
                ImmutableList.of(new CanonicalPlanWithInfo(
                        new CanonicalPlan(
                                new ValuesNode(Optional.empty(), new PlanNodeId("0"), ImmutableList.of(), ImmutableList.of(), Optional.empty()),
                                PlanCanonicalizationStrategy.DEFAULT),
                        new PlanNodeCanonicalInfo("a", ImmutableList.of()))));
    }
}
