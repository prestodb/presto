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
package com.facebook.presto.sql.planner.plan;

import com.facebook.presto.metadata.HandleJsonModule;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.statistics.ColumnStatisticMetadata;
import com.facebook.presto.spi.statistics.ColumnStatisticType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.type.TypeDeserializer;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonModule;
import org.testng.annotations.Test;

import java.util.UUID;

import static com.facebook.presto.spi.statistics.TableStatisticType.ROW_COUNT;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.json.JsonBinder.jsonBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static org.testng.Assert.assertEquals;

public class TestStatisticsWriterNode
{
    private static final ImmutableList<String> COLUMNS = ImmutableList.of("", "col1", "$:###:;", "abc+dddd___");

    @Test
    public void testJsonCodec()
            throws Exception
    {
        JsonCodec<StatisticsWriterNode> jsonCodec = getJsonCodec();
        StatisticsWriterNode expected = createStatisticsWriterNode();
        StatisticsWriterNode deserialized = jsonCodec.fromJson(jsonCodec.toJson(expected));
        assertEquals(deserialized.getTarget(), expected.getTarget());
        assertEquals(deserialized.getRowCountVariable(), expected.getRowCountVariable());
        assertEquals(deserialized.isRowCountEnabled(), expected.isRowCountEnabled());
        assertEquals(deserialized.getDescriptor(), expected.getDescriptor());
    }

    private static PlanNodeId newId()
    {
        return new PlanNodeId(UUID.randomUUID().toString());
    }

    private static StatisticAggregationsDescriptor<VariableReferenceExpression> createTestDescriptor()
    {
        StatisticAggregationsDescriptor.Builder<VariableReferenceExpression> builder = StatisticAggregationsDescriptor.builder();
        PlanVariableAllocator variableAllocator = new PlanVariableAllocator();
        for (String column : COLUMNS) {
            for (ColumnStatisticType type : ColumnStatisticType.values()) {
                builder.addColumnStatistic(new ColumnStatisticMetadata(column, type), testVariable(variableAllocator));
            }
            builder.addGrouping(column, testVariable(variableAllocator));
        }
        builder.addTableStatistic(ROW_COUNT, testVariable(variableAllocator));
        return builder.build();
    }

    private static VariableReferenceExpression testVariable(PlanVariableAllocator allocator)
    {
        return allocator.newVariable("test", BIGINT);
    }

    private StatisticsWriterNode createStatisticsWriterNode()
    {
        PlanVariableAllocator variableAllocator = new PlanVariableAllocator();

        return new StatisticsWriterNode(
                newId(),
                new ValuesNode(newId(), COLUMNS.stream().map(column -> new VariableReferenceExpression(column, BIGINT)).collect(toImmutableList()), ImmutableList.of()),
                new StatisticsWriterNode.TestWriteStatisticsHandle(),
                variableAllocator.newVariable("count", BIGINT),
                true,
                createTestDescriptor());
    }

    private JsonCodec<StatisticsWriterNode> getJsonCodec()
            throws Exception
    {
        Module module = binder -> {
            SqlParser sqlParser = new SqlParser();
            TypeManager typeManager = new TypeRegistry();
            binder.install(new JsonModule());
            binder.install(new HandleJsonModule());
            binder.bind(SqlParser.class).toInstance(sqlParser);
            binder.bind(TypeManager.class).toInstance(typeManager);
            newSetBinder(binder, Type.class);
            jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
            jsonCodecBinder(binder).bindJsonCodec(StatisticsWriterNode.class);
        };
        Bootstrap app = new Bootstrap(ImmutableList.of(module));
        Injector injector = app
                .strictConfig()
                .doNotInitializeLogging()
                .quiet()
                .initialize();
        return injector.getInstance(new Key<JsonCodec<StatisticsWriterNode>>() {});
    }
}
