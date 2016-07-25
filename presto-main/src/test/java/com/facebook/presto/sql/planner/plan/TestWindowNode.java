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

import com.facebook.presto.metadata.FunctionKind;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.server.SliceDeserializer;
import com.facebook.presto.server.SliceSerializer;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.sql.Serialization;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FrameBound;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.WindowFrame;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.slice.Slice;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static org.testng.Assert.assertEquals;

public class TestWindowNode
{
    private SymbolAllocator symbolAllocator;
    private ValuesNode sourceNode;
    private Symbol columnA;
    private Symbol columnB;
    private Symbol columnC;

    private final ObjectMapper objectMapper;

    public TestWindowNode()
    {
        // dependencies copied from ServerMainModule.java to avoid depending on whole ServerMainModule here
        SqlParser sqlParser = new SqlParser();
        ObjectMapperProvider provider = new ObjectMapperProvider();
        provider.setJsonSerializers(ImmutableMap.of(
                Slice.class, new SliceSerializer(),
                Expression.class, new Serialization.ExpressionSerializer()));
        provider.setJsonDeserializers(ImmutableMap.of(
                Slice.class, new SliceDeserializer(),
                Expression.class, new Serialization.ExpressionDeserializer(sqlParser),
                FunctionCall.class, new Serialization.FunctionCallDeserializer(sqlParser)));
        objectMapper = provider.get();
    }

    @BeforeMethod
    public void setUp()
    {
        symbolAllocator = new SymbolAllocator();
        columnA = symbolAllocator.newSymbol("a", BIGINT);
        columnB = symbolAllocator.newSymbol("b", BIGINT);
        columnC = symbolAllocator.newSymbol("c", BIGINT);

        sourceNode = new ValuesNode(
                newId(),
                ImmutableList.of(columnA, columnB, columnC),
                ImmutableList.of());
    }

    @Test
    public void testSerializationRoundtrip()
            throws Exception
    {
        Symbol windowSymbol = symbolAllocator.newSymbol("sum", BIGINT);
        Signature signature = new Signature(
                "sum",
                FunctionKind.WINDOW,
                ImmutableList.of(),
                ImmutableList.of(),
                BIGINT.getTypeSignature(),
                ImmutableList.of(BIGINT.getTypeSignature()),
                false);
        FunctionCall functionCall = new FunctionCall(QualifiedName.of("sum"), ImmutableList.of(columnC.toSymbolReference()));
        WindowNode.Frame frame = new WindowNode.Frame(
                WindowFrame.Type.RANGE,
                FrameBound.Type.UNBOUNDED_PRECEDING,
                Optional.empty(),
                FrameBound.Type.UNBOUNDED_FOLLOWING,
                Optional.empty());

        PlanNodeId id = newId();
        WindowNode.Specification specification = new WindowNode.Specification(
                ImmutableList.of(columnA),
                ImmutableList.of(columnB),
                ImmutableMap.of(columnB, SortOrder.ASC_NULLS_FIRST));
        Map<Symbol, WindowNode.Function> functions = ImmutableMap.of(windowSymbol, new WindowNode.Function(functionCall, signature, frame));
        Optional<Symbol> hashSymbol = Optional.of(columnB);
        Set<Symbol> prePartitionedInputs = ImmutableSet.of(columnA);
        WindowNode windowNode = new WindowNode(
                id,
                sourceNode,
                specification,
                functions,
                hashSymbol,
                prePartitionedInputs,
                0);

        String json = objectMapper.writeValueAsString(windowNode);

        WindowNode actualNode = objectMapper.readValue(json, WindowNode.class);

        assertEquals(actualNode.getId(), windowNode.getId());
        assertEquals(actualNode.getSpecification(), windowNode.getSpecification());
        assertEquals(actualNode.getWindowFunctions(), windowNode.getWindowFunctions());
        assertEquals(actualNode.getFrames(), windowNode.getFrames());
        assertEquals(actualNode.getHashSymbol(), windowNode.getHashSymbol());
        assertEquals(actualNode.getPrePartitionedInputs(), windowNode.getPrePartitionedInputs());
        assertEquals(actualNode.getPreSortedOrderPrefix(), windowNode.getPreSortedOrderPrefix());
    }

    private static PlanNodeId newId()
    {
        return new PlanNodeId(UUID.randomUUID().toString());
    }
}
