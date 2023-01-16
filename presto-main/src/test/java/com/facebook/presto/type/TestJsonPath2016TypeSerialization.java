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
package com.facebook.presto.type;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.TestingBlockEncodingSerde;
import com.facebook.presto.common.type.TestingTypeManager;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.json.ir.IrAbsMethod;
import com.facebook.presto.json.ir.IrArithmeticBinary;
import com.facebook.presto.json.ir.IrArithmeticUnary;
import com.facebook.presto.json.ir.IrArrayAccessor;
import com.facebook.presto.json.ir.IrArrayAccessor.Subscript;
import com.facebook.presto.json.ir.IrCeilingMethod;
import com.facebook.presto.json.ir.IrConstantJsonSequence;
import com.facebook.presto.json.ir.IrContextVariable;
import com.facebook.presto.json.ir.IrDatetimeMethod;
import com.facebook.presto.json.ir.IrDoubleMethod;
import com.facebook.presto.json.ir.IrFloorMethod;
import com.facebook.presto.json.ir.IrJsonNull;
import com.facebook.presto.json.ir.IrJsonPath;
import com.facebook.presto.json.ir.IrKeyValueMethod;
import com.facebook.presto.json.ir.IrLastIndexVariable;
import com.facebook.presto.json.ir.IrLiteral;
import com.facebook.presto.json.ir.IrMemberAccessor;
import com.facebook.presto.json.ir.IrNamedJsonVariable;
import com.facebook.presto.json.ir.IrNamedValueVariable;
import com.facebook.presto.json.ir.IrSizeMethod;
import com.facebook.presto.json.ir.IrTypeMethod;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DecimalType.createDecimalType;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.TimeType.TIME;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.json.ir.IrArithmeticBinary.Operator.ADD;
import static com.facebook.presto.json.ir.IrArithmeticBinary.Operator.MULTIPLY;
import static com.facebook.presto.json.ir.IrArithmeticUnary.Sign.MINUS;
import static com.facebook.presto.json.ir.IrArithmeticUnary.Sign.PLUS;
import static com.facebook.presto.json.ir.IrConstantJsonSequence.EMPTY_SEQUENCE;
import static com.facebook.presto.json.ir.IrConstantJsonSequence.singletonSequence;
import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;

public class TestJsonPath2016TypeSerialization
{
    private static final Type JSON_PATH_2016 = new JsonPath2016Type(new TypeDeserializer(new TestingTypeManager()), new TestingBlockEncodingSerde());

    @Test
    public void testJsonPathMode()
    {
        assertJsonRoundTrip(new IrJsonPath(true, new IrJsonNull()));
        assertJsonRoundTrip(new IrJsonPath(false, new IrJsonNull()));
    }

    @Test
    public void testLiterals()
    {
        assertJsonRoundTrip(new IrJsonPath(true, new IrLiteral(createDecimalType(2, 1), 1L)));
        assertJsonRoundTrip(new IrJsonPath(true, new IrLiteral(DOUBLE, 1e0)));
        assertJsonRoundTrip(new IrJsonPath(true, new IrLiteral(INTEGER, 1L)));
        assertJsonRoundTrip(new IrJsonPath(true, new IrLiteral(BIGINT, 1000000000000L)));
        assertJsonRoundTrip(new IrJsonPath(true, new IrLiteral(VARCHAR, utf8Slice("some_text"))));
        assertJsonRoundTrip(new IrJsonPath(true, new IrLiteral(BOOLEAN, false)));
    }

    @Test
    public void testContextVariable()
    {
        assertJsonRoundTrip(new IrJsonPath(true, new IrContextVariable(Optional.empty())));
        assertJsonRoundTrip(new IrJsonPath(true, new IrContextVariable(Optional.of(DOUBLE))));
    }

    @Test
    public void testNamedVariables()
    {
        // json variable
        assertJsonRoundTrip(new IrJsonPath(true, new IrNamedJsonVariable(5, Optional.empty())));
        assertJsonRoundTrip(new IrJsonPath(true, new IrNamedJsonVariable(5, Optional.of(DOUBLE))));

        // SQL value variable
        assertJsonRoundTrip(new IrJsonPath(true, new IrNamedValueVariable(5, Optional.of(DOUBLE))));
    }

    @Test
    public void testMethods()
    {
        assertJsonRoundTrip(new IrJsonPath(true, new IrAbsMethod(new IrLiteral(DOUBLE, 1e0), Optional.of(DOUBLE))));
        assertJsonRoundTrip(new IrJsonPath(true, new IrCeilingMethod(new IrLiteral(DOUBLE, 1e0), Optional.of(DOUBLE))));
        assertJsonRoundTrip(new IrJsonPath(true, new IrDatetimeMethod(new IrLiteral(BIGINT, 1L), Optional.of("some_time_format"), Optional.of(TIME))));
        assertJsonRoundTrip(new IrJsonPath(true, new IrDoubleMethod(new IrLiteral(BIGINT, 1L), Optional.of(DOUBLE))));
        assertJsonRoundTrip(new IrJsonPath(true, new IrFloorMethod(new IrLiteral(DOUBLE, 1e0), Optional.of(DOUBLE))));
        assertJsonRoundTrip(new IrJsonPath(true, new IrKeyValueMethod(new IrJsonNull())));
        assertJsonRoundTrip(new IrJsonPath(true, new IrSizeMethod(new IrJsonNull(), Optional.of(INTEGER))));
        assertJsonRoundTrip(new IrJsonPath(true, new IrTypeMethod(new IrJsonNull(), Optional.of(createVarcharType(7)))));
    }

    @Test
    public void testArrayAccessor()
    {
        // wildcard accessor
        assertJsonRoundTrip(new IrJsonPath(true, new IrArrayAccessor(new IrJsonNull(), ImmutableList.of(), Optional.empty())));

        // with subscripts based on literals
        assertJsonRoundTrip(new IrJsonPath(true, new IrArrayAccessor(
                new IrJsonNull(),
                ImmutableList.of(
                        new Subscript(new IrLiteral(INTEGER, 0L), Optional.of(new IrLiteral(INTEGER, 1L))),
                        new Subscript(new IrLiteral(INTEGER, 3L), Optional.of(new IrLiteral(INTEGER, 5L))),
                        new Subscript(new IrLiteral(INTEGER, 7L), Optional.empty())),
                Optional.of(VARCHAR))));

        // with LAST index variable
        assertJsonRoundTrip(new IrJsonPath(true, new IrArrayAccessor(
                new IrJsonNull(),
                ImmutableList.of(new Subscript(new IrLastIndexVariable(Optional.of(INTEGER)), Optional.empty())),
                Optional.empty())));
    }

    @Test
    public void testMemberAccessor()
    {
        // wildcard accessor
        assertJsonRoundTrip(new IrJsonPath(true, new IrMemberAccessor(new IrJsonNull(), Optional.empty(), Optional.empty())));

        // accessor by field name
        assertJsonRoundTrip(new IrJsonPath(true, new IrMemberAccessor(new IrJsonNull(), Optional.of("some_key"), Optional.of(BIGINT))));
    }

    @Test
    public void testArithmeticBinary()
    {
        assertJsonRoundTrip(new IrJsonPath(true, new IrArithmeticBinary(ADD, new IrJsonNull(), new IrJsonNull(), Optional.empty())));

        assertJsonRoundTrip(new IrJsonPath(true, new IrArithmeticBinary(
                ADD,
                new IrLiteral(INTEGER, 1L),
                new IrLiteral(BIGINT, 2L),
                Optional.of(BIGINT))));
    }

    @Test
    public void testArithmeticUnary()
    {
        assertJsonRoundTrip(new IrJsonPath(true, new IrArithmeticUnary(PLUS, new IrJsonNull(), Optional.empty())));
        assertJsonRoundTrip(new IrJsonPath(true, new IrArithmeticUnary(MINUS, new IrJsonNull(), Optional.empty())));
        assertJsonRoundTrip(new IrJsonPath(true, new IrArithmeticUnary(MINUS, new IrLiteral(INTEGER, 1L), Optional.of(INTEGER))));
    }

    @Test
    public void testConstantJsonSequence()
    {
        // empty sequence
        assertJsonRoundTrip(new IrJsonPath(true, EMPTY_SEQUENCE));

        // singleton sequence
        assertJsonRoundTrip(new IrJsonPath(true, singletonSequence(NullNode.getInstance(), Optional.empty())));
        assertJsonRoundTrip(new IrJsonPath(true, singletonSequence(BooleanNode.TRUE, Optional.of(BOOLEAN))));

        // long sequence
        assertJsonRoundTrip(new IrJsonPath(true, new IrConstantJsonSequence(
                ImmutableList.of(IntNode.valueOf(1), IntNode.valueOf(2), IntNode.valueOf(3)),
                Optional.of(INTEGER))));
    }

    @Test
    public void testNestedStructure()
    {
        assertJsonRoundTrip(new IrJsonPath(
                true,
                new IrTypeMethod(
                        new IrArithmeticBinary(
                                MULTIPLY,
                                new IrArithmeticUnary(MINUS, new IrAbsMethod(new IrFloorMethod(new IrLiteral(INTEGER, 1L), Optional.of(INTEGER)), Optional.of(INTEGER)), Optional.of(INTEGER)),
                                new IrCeilingMethod(new IrMemberAccessor(new IrContextVariable(Optional.empty()), Optional.of("some_key"), Optional.of(BIGINT)), Optional.of(BIGINT)),
                                Optional.of(BIGINT)),
                        Optional.of(createVarcharType(7)))));
    }

    private static void assertJsonRoundTrip(IrJsonPath object)
    {
        BlockBuilder blockBuilder = JSON_PATH_2016.createBlockBuilder(null, 1);
        JSON_PATH_2016.writeObject(blockBuilder, object);
        Block serialized = blockBuilder.build();
        Object deserialized = JSON_PATH_2016.getObject(serialized, 0);
        assertEquals(deserialized, object);
    }
}
