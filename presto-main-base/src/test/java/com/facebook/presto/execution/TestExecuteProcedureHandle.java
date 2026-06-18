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
import com.facebook.drift.codec.guice.ThriftCodecModule;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.execution.scheduler.ExecutionWriterTarget.ExecuteProcedureHandle;
import com.facebook.presto.metadata.DistributedProcedureHandle;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.HandleJsonModule;
import com.facebook.presto.metadata.HandleResolver;
import com.facebook.presto.server.SliceDeserializer;
import com.facebook.presto.server.SliceSerializer;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.Serialization;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.testing.TestingHandle;
import com.facebook.presto.testing.TestingHandleResolver;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.facebook.presto.type.TypeDeserializer;
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import java.util.UUID;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.airlift.json.JsonBinder.jsonBinder;
import static com.facebook.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static org.testng.Assert.assertEquals;

public class TestExecuteProcedureHandle
{
    @Test
    public void testExecuteProcedureHandleRoundTrip()
    {
        String catalogName = "test_catalog";
        JsonCodec<ExecuteProcedureHandle> codec = createJsonCodec(catalogName);
        UUID uuid = UUID.randomUUID();
        ExecuteProcedureHandle expected = createExecuteProcedureHandle(catalogName, uuid);
        ExecuteProcedureHandle actual = codec.fromJson(codec.toJson(expected));

        assertEquals(actual.getProcedureName(), expected.getProcedureName());
        assertEquals(actual.getSchemaTableName(), expected.getSchemaTableName());
        assertEquals(actual.getHandle().getClass(), expected.getHandle().getClass());
        assertEquals(actual.getHandle().getConnectorId(), expected.getHandle().getConnectorId());
        assertEquals(actual.getHandle().getTransactionHandle(), expected.getHandle().getTransactionHandle());
        assertEquals(actual.getHandle().getConnectorHandle(), expected.getHandle().getConnectorHandle());
    }

    private static JsonCodec<ExecuteProcedureHandle> createJsonCodec(String catalogName)
    {
        Module module = binder -> {
            SqlParser sqlParser = new SqlParser();
            FunctionAndTypeManager functionAndTypeManager = createTestFunctionAndTypeManager();
            binder.install(new JsonModule());
            binder.install(new HandleJsonModule());
            binder.bind(ConnectorManager.class).toProvider(() -> null).in(Scopes.SINGLETON);
            binder.install(new ThriftCodecModule());
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
            jsonCodecBinder(binder).bindJsonCodec(ExecuteProcedureHandle.class);
        };
        Bootstrap app = new Bootstrap(ImmutableList.of(module));
        Injector injector = app
                .doNotInitializeLogging()
                .quiet()
                .initialize();
        injector.getInstance(HandleResolver.class)
                .addConnectorName(catalogName, new TestingHandleResolver());
        return injector.getInstance(new Key<JsonCodec<ExecuteProcedureHandle>>() {});
    }

    private static ExecuteProcedureHandle createExecuteProcedureHandle(String catalogName, UUID uuid)
    {
        DistributedProcedureHandle distributedProcedureHandle = new DistributedProcedureHandle(
                new ConnectorId(catalogName),
                new TestingTransactionHandle(uuid),
                TestingHandle.INSTANCE);
        return new ExecuteProcedureHandle(distributedProcedureHandle,
                new SchemaTableName("schema1", "table1"),
                QualifiedObjectName.valueOf(catalogName, "schema1", "table1"));
    }
}
