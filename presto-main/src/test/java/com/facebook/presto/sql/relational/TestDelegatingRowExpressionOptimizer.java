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
package com.facebook.presto.sql.relational;

import com.facebook.airlift.node.NodeInfo;
import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.common.block.IntArrayBlock;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.sql.expressions.JsonCodecRowExpressionSerde;
import com.facebook.presto.sql.expressions.ExpressionManager;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.airlift.testing.Assertions.assertInstanceOf;
import static com.facebook.presto.block.BlockAssertions.toValues;
import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.JsonType.JSON;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.metadata.CastType.CAST;
import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level.OPTIMIZED;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IF;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;

public class TestDelegatingRowExpressionOptimizer
{
    private DelegatingRowExpressionOptimizer optimizer;
    private static final MetadataManager METADATA = MetadataManager.createTestMetadataManager();

    @BeforeClass
    public void setUp()
    {
        InMemoryNodeManager inMemoryNodeManager = new InMemoryNodeManager();
        inMemoryNodeManager.addNode(new ConnectorId("test"), new InternalNode("id", URI.create("id"), new NodeVersion("test"), false));
        ExpressionManager expressionManager = new ExpressionManager(inMemoryNodeManager, METADATA.getFunctionAndTypeManager(), new NodeInfo("env"), new JsonCodecRowExpressionSerde(jsonCodec(RowExpression.class)));
        optimizer = new DelegatingRowExpressionOptimizer(METADATA, expressionManager);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        optimizer = null;
    }

    @Test
    public void testIfConstantOptimization()
    {
        assertEquals(optimize(ifExpression(constant(true, BOOLEAN), 1L, 2L)), constant(1L, BIGINT));
        assertEquals(optimize(ifExpression(constant(false, BOOLEAN), 1L, 2L)), constant(2L, BIGINT));
        assertEquals(optimize(ifExpression(constant(null, BOOLEAN), 1L, 2L)), constant(2L, BIGINT));

        FunctionHandle bigintEquals = METADATA.getFunctionAndTypeManager().resolveOperator(EQUAL, fromTypes(BIGINT, BIGINT));
        RowExpression condition = new CallExpression(EQUAL.name(), bigintEquals, BOOLEAN, ImmutableList.of(constant(3L, BIGINT), constant(3L, BIGINT)));
        assertEquals(optimize(ifExpression(condition, 1L, 2L)), constant(1L, BIGINT));
    }

    @Test
    public void testCastWithJsonParseOptimization()
    {
        FunctionHandle jsonParseFunctionHandle = METADATA.getFunctionAndTypeManager().lookupFunction("json_parse", fromTypes(VARCHAR));

        // constant
        FunctionHandle jsonCastFunctionHandle = METADATA.getFunctionAndTypeManager().lookupCast(CAST, JSON, METADATA.getFunctionAndTypeManager().getType(parseTypeSignature("array(integer)")));
        RowExpression jsonCastExpression = new CallExpression(CAST.name(), jsonCastFunctionHandle, new ArrayType(INTEGER), ImmutableList.of(call("json_parse", jsonParseFunctionHandle, JSON, constant(utf8Slice("[1, 2]"), VARCHAR))));
        RowExpression resultExpression = optimize(jsonCastExpression);
        assertInstanceOf(resultExpression, ConstantExpression.class);
        Object resultValue = ((ConstantExpression) resultExpression).getValue();
        assertInstanceOf(resultValue, IntArrayBlock.class);
        assertEquals(toValues(INTEGER, (IntArrayBlock) resultValue), ImmutableList.of(1, 2));
    }

    private static RowExpression ifExpression(RowExpression condition, long trueValue, long falseValue)
    {
        return new SpecialFormExpression(IF, BIGINT, ImmutableList.of(condition, constant(trueValue, BIGINT), constant(falseValue, BIGINT)));
    }

    private RowExpression optimize(RowExpression expression)
    {
        return optimizer.optimize(expression, OPTIMIZED, SESSION);
    }
}
