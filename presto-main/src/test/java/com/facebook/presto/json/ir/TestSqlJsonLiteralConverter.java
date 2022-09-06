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
package com.facebook.presto.json.ir;

import com.facebook.presto.json.ir.SqlJsonLiteralConverter.JsonLiteralConversionError;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BigIntegerNode;
import com.fasterxml.jackson.databind.node.BinaryNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DecimalNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.FloatNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ShortNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.assertj.core.api.AssertProvider;
import org.assertj.core.api.RecursiveComparisonAssert;
import org.assertj.core.api.recursive.comparison.RecursiveComparisonConfiguration;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.NoSuchElementException;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.CharType.createCharType;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DecimalType.createDecimalType;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.json.ir.SqlJsonLiteralConverter.getJsonNode;
import static com.facebook.presto.json.ir.SqlJsonLiteralConverter.getNumericTypedValue;
import static com.facebook.presto.json.ir.SqlJsonLiteralConverter.getTextTypedValue;
import static com.facebook.presto.json.ir.SqlJsonLiteralConverter.getTypedValue;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Float.floatToIntBits;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestSqlJsonLiteralConverter
{
    private static final RecursiveComparisonConfiguration COMPARISON_CONFIGURATION = RecursiveComparisonConfiguration.builder().withStrictTypeChecking(true).build();

    @Test
    public void testNumberToJson()
    {
        assertThat(json(new TypedValue(TINYINT, 1L)))
                .isEqualTo(ShortNode.valueOf((short) 1));

        assertThat(json(new TypedValue(SMALLINT, 1L)))
                .isEqualTo(ShortNode.valueOf((short) 1));

        assertThat(json(new TypedValue(INTEGER, 1L)))
                .isEqualTo(IntNode.valueOf(1));

        assertThat(json(new TypedValue(BIGINT, 1L)))
                .isEqualTo(LongNode.valueOf(1));

        assertThat(json(new TypedValue(DOUBLE, 1e0)))
                .isEqualTo(DoubleNode.valueOf(1));

        assertThat(json(new TypedValue(DOUBLE, Double.NaN)))
                .isEqualTo(DoubleNode.valueOf(Double.NaN));

        assertThat(json(new TypedValue(DOUBLE, Double.NEGATIVE_INFINITY)))
                .isEqualTo(DoubleNode.valueOf(Double.NEGATIVE_INFINITY));

        assertThat(json(new TypedValue(DOUBLE, Double.POSITIVE_INFINITY)))
                .isEqualTo(DoubleNode.valueOf(Double.POSITIVE_INFINITY));

        assertThat(json(new TypedValue(REAL, floatToIntBits(1F))))
                .isEqualTo(FloatNode.valueOf(1F));

        assertThat(json(new TypedValue(REAL, floatToIntBits(Float.NaN))))
                .isEqualTo(FloatNode.valueOf(Float.NaN));

        assertThat(json(new TypedValue(REAL, floatToIntBits(Float.NEGATIVE_INFINITY))))
                .isEqualTo(FloatNode.valueOf(Float.NEGATIVE_INFINITY));

        assertThat(json(new TypedValue(REAL, floatToIntBits(Float.POSITIVE_INFINITY))))
                .isEqualTo(FloatNode.valueOf(Float.POSITIVE_INFINITY));

        assertThat(json(new TypedValue(createDecimalType(2, 1), 1L)))
                .isEqualTo(DecimalNode.valueOf(new BigDecimal(BigInteger.ONE, 1)));
    }

    @Test
    public void testCharacterStringToJson()
    {
        assertThat(json(new TypedValue(VARCHAR, utf8Slice("abc"))))
                .isEqualTo(TextNode.valueOf("abc"));

        assertThat(json(new TypedValue(createVarcharType(10), utf8Slice("abc"))))
                .isEqualTo(TextNode.valueOf("abc"));

        assertThat(json(new TypedValue(createCharType(10), utf8Slice("abc"))))
                .isEqualTo(TextNode.valueOf("abc       "));
    }

    @Test
    public void testBooleanToJson()
    {
        assertThat(json(new TypedValue(BOOLEAN, true)))
                .isEqualTo(BooleanNode.TRUE);

        assertThat(json(new TypedValue(BOOLEAN, false)))
                .isEqualTo(BooleanNode.FALSE);
    }

    @Test
    public void testNoConversionToJson()
    {
        // datetime types are supported in the path engine, but they are not supported in JSON
        assertThat(getJsonNode(new TypedValue(DATE, 1L)))
                .isEqualTo(Optional.empty());
    }

    @Test
    public void testJsonToNumber()
    {
        BigInteger bigValue = BigInteger.valueOf(1000000000000000000L).multiply(BigInteger.valueOf(1000000000000000000L));

        assertThat(typedValueResult(BigIntegerNode.valueOf(BigInteger.ONE)))
                .isEqualTo(new TypedValue(INTEGER, 1L));

        assertThat(typedValueResult(BigIntegerNode.valueOf(BigInteger.valueOf(1000000000000000000L))))
                .isEqualTo(new TypedValue(BIGINT, 1000000000000000000L));

        assertThatThrownBy(() -> getTypedValue(BigIntegerNode.valueOf(bigValue)))
                .isInstanceOf(JsonLiteralConversionError.class)
                .hasMessage("cannot convert 1000000000000000000000000000000000000 to Trino value (value too big)");

        assertThat(typedValueResult(DecimalNode.valueOf(BigDecimal.ONE)))
                .isEqualTo(new TypedValue(createDecimalType(1, 0), 1L));

        assertThatThrownBy(() -> getTypedValue(BigIntegerNode.valueOf(bigValue.multiply(bigValue))))
                .isInstanceOf(JsonLiteralConversionError.class)
                .hasMessage("cannot convert 1000000000000000000000000000000000000000000000000000000000000000000000000 to Trino value (value too big)");

        assertThat(typedValueResult(DoubleNode.valueOf(1e0)))
                .isEqualTo(new TypedValue(DOUBLE, 1e0));

        assertThat(typedValueResult(DoubleNode.valueOf(Double.NEGATIVE_INFINITY)))
                .isEqualTo(new TypedValue(DOUBLE, Double.NEGATIVE_INFINITY));

        assertThat(typedValueResult(DoubleNode.valueOf(Double.POSITIVE_INFINITY)))
                .isEqualTo(new TypedValue(DOUBLE, Double.POSITIVE_INFINITY));

        assertThat(typedValueResult(FloatNode.valueOf(1F)))
                .isEqualTo(new TypedValue(REAL, floatToIntBits(1F)));

        assertThat(typedValueResult(FloatNode.valueOf(Float.NEGATIVE_INFINITY)))
                .isEqualTo(new TypedValue(REAL, floatToIntBits(Float.NEGATIVE_INFINITY)));

        assertThat(typedValueResult(FloatNode.valueOf(Float.POSITIVE_INFINITY)))
                .isEqualTo(new TypedValue(REAL, floatToIntBits(Float.POSITIVE_INFINITY)));

        assertThat(typedValueResult(IntNode.valueOf(1)))
                .isEqualTo(new TypedValue(INTEGER, 1L));

        assertThat(typedValueResult(LongNode.valueOf(1)))
                .isEqualTo(new TypedValue(BIGINT, 1L));

        assertThat(typedValueResult(ShortNode.valueOf((short) 1)))
                .isEqualTo(new TypedValue(SMALLINT, 1L));
    }

    @Test
    public void testJsonToCharacterString()
    {
        assertThat(typedValueResult(TextNode.valueOf("abc   ")))
                .isEqualTo(new TypedValue(VARCHAR, utf8Slice("abc   ")));
    }

    @Test
    public void testJsonToBoolean()
    {
        assertThat(typedValueResult(BooleanNode.TRUE))
                .isEqualTo(new TypedValue(BOOLEAN, true));

        assertThat(typedValueResult(BooleanNode.FALSE))
                .isEqualTo(new TypedValue(BOOLEAN, false));
    }

    @Test
    public void testJsonToIncompatibleType()
    {
        assertThat(getNumericTypedValue(TextNode.valueOf("abc")))
                .isEqualTo(Optional.empty());

        assertThat(getTextTypedValue(NullNode.instance))
                .isEqualTo(Optional.empty());
    }

    @Test
    public void testNoConversionFromJson()
    {
        // unsupported node type
        assertThat(getTextTypedValue(BinaryNode.valueOf(new byte[] {})))
                .isEqualTo(Optional.empty());

        // not a value node
        assertThat(getTextTypedValue(MissingNode.getInstance()))
                .isEqualTo(Optional.empty());

        assertThat(getTextTypedValue(new ObjectNode(JsonNodeFactory.instance)))
                .isEqualTo(Optional.empty());

        assertThat(getTextTypedValue(new ArrayNode(JsonNodeFactory.instance)))
                .isEqualTo(Optional.empty());
    }

    private static JsonNode json(TypedValue value)
    {
        return getJsonNode(value).orElseThrow(NoSuchElementException::new);
    }

    private static AssertProvider<? extends RecursiveComparisonAssert<?>> typedValueResult(JsonNode node)
    {
        return () -> new RecursiveComparisonAssert<>(getTypedValue(node).orElseThrow(NoSuchElementException::new), COMPARISON_CONFIGURATION);
    }
}
