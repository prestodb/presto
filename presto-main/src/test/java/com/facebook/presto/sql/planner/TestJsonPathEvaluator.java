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
package com.facebook.presto.sql.planner;

import com.facebook.presto.Session;
import com.facebook.presto.json.ir.IrJsonPath;
import com.facebook.presto.json.ir.IrPredicate;
import com.facebook.presto.json.ir.TypedValue;
import com.facebook.presto.sql.planner.JsonPathEvaluator.PathEvaluationError;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DecimalNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ShortNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.assertj.core.api.AssertProvider;
import org.assertj.core.api.RecursiveComparisonAssert;
import org.assertj.core.api.recursive.comparison.RecursiveComparisonConfiguration;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.CharType.createCharType;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DecimalType.createDecimalType;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.json.JsonEmptySequenceNode.EMPTY_SEQUENCE;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.sql.planner.PathNodes.abs;
import static com.facebook.presto.sql.planner.PathNodes.add;
import static com.facebook.presto.sql.planner.PathNodes.arrayAccessor;
import static com.facebook.presto.sql.planner.PathNodes.at;
import static com.facebook.presto.sql.planner.PathNodes.ceiling;
import static com.facebook.presto.sql.planner.PathNodes.conjunction;
import static com.facebook.presto.sql.planner.PathNodes.contextVariable;
import static com.facebook.presto.sql.planner.PathNodes.currentItem;
import static com.facebook.presto.sql.planner.PathNodes.disjunction;
import static com.facebook.presto.sql.planner.PathNodes.divide;
import static com.facebook.presto.sql.planner.PathNodes.emptySequence;
import static com.facebook.presto.sql.planner.PathNodes.equal;
import static com.facebook.presto.sql.planner.PathNodes.exists;
import static com.facebook.presto.sql.planner.PathNodes.filter;
import static com.facebook.presto.sql.planner.PathNodes.floor;
import static com.facebook.presto.sql.planner.PathNodes.greaterThan;
import static com.facebook.presto.sql.planner.PathNodes.greaterThanOrEqual;
import static com.facebook.presto.sql.planner.PathNodes.isUnknown;
import static com.facebook.presto.sql.planner.PathNodes.jsonNull;
import static com.facebook.presto.sql.planner.PathNodes.jsonVariable;
import static com.facebook.presto.sql.planner.PathNodes.keyValue;
import static com.facebook.presto.sql.planner.PathNodes.last;
import static com.facebook.presto.sql.planner.PathNodes.lessThan;
import static com.facebook.presto.sql.planner.PathNodes.lessThanOrEqual;
import static com.facebook.presto.sql.planner.PathNodes.literal;
import static com.facebook.presto.sql.planner.PathNodes.memberAccessor;
import static com.facebook.presto.sql.planner.PathNodes.minus;
import static com.facebook.presto.sql.planner.PathNodes.modulus;
import static com.facebook.presto.sql.planner.PathNodes.multiply;
import static com.facebook.presto.sql.planner.PathNodes.negation;
import static com.facebook.presto.sql.planner.PathNodes.notEqual;
import static com.facebook.presto.sql.planner.PathNodes.path;
import static com.facebook.presto.sql.planner.PathNodes.plus;
import static com.facebook.presto.sql.planner.PathNodes.range;
import static com.facebook.presto.sql.planner.PathNodes.sequence;
import static com.facebook.presto.sql.planner.PathNodes.singletonSequence;
import static com.facebook.presto.sql.planner.PathNodes.size;
import static com.facebook.presto.sql.planner.PathNodes.startsWith;
import static com.facebook.presto.sql.planner.PathNodes.subtract;
import static com.facebook.presto.sql.planner.PathNodes.toDouble;
import static com.facebook.presto.sql.planner.PathNodes.type;
import static com.facebook.presto.sql.planner.PathNodes.variable;
import static com.facebook.presto.sql.planner.PathNodes.wildcardArrayAccessor;
import static com.facebook.presto.sql.planner.PathNodes.wildcardMemberAccessor;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestJsonPathEvaluator
{
    private static final RecursiveComparisonConfiguration COMPARISON_CONFIGURATION = RecursiveComparisonConfiguration.builder().withStrictTypeChecking(true).build();

    private static final Map<String, Object> PARAMETERS = ImmutableMap.<String, Object>builder()
            .put("tinyint_parameter", new TypedValue(TINYINT, 1L))
            .put("bigint_parameter", new TypedValue(BIGINT, -2L))
            .put("short_decimal_parameter", new TypedValue(createDecimalType(3, 1), -123L))
            .put("double_parameter", new TypedValue(DOUBLE, 5e0))
            .put("string_parameter", new TypedValue(createCharType(5), utf8Slice("xyz")))
            .put("boolean_parameter", new TypedValue(BOOLEAN, true))
            .put("date_parameter", new TypedValue(DATE, 1234L))
            .put("empty_sequence_parameter", EMPTY_SEQUENCE)
            .put("null_parameter", NullNode.instance)
            .put("json_number_parameter", IntNode.valueOf(-6))
            .put("json_text_parameter", TextNode.valueOf("JSON text"))
            .put("json_boolean_parameter", BooleanNode.FALSE)
            .put("json_array_parameter", new ArrayNode(JsonNodeFactory.instance, ImmutableList.of(TextNode.valueOf("element"), DoubleNode.valueOf(7e0), NullNode.instance)))
            .put("json_object_parameter", new ObjectNode(JsonNodeFactory.instance, ImmutableMap.of("key1", TextNode.valueOf("bound_value"), "key2", NullNode.instance)))
            .build();

    @Test
    public void testLiterals()
    {
        assertThat(pathResult(
                NullNode.instance,
                path(true, literal(BIGINT, 1L))))
                .isEqualTo(singletonSequence(new TypedValue(BIGINT, 1L)));

        assertThat(pathResult(
                NullNode.instance,
                path(true, literal(createVarcharType(5), utf8Slice("abc")))))
                .isEqualTo(singletonSequence(new TypedValue(createVarcharType(5), utf8Slice("abc"))));

        assertThat(pathResult(
                NullNode.instance,
                path(true, literal(BOOLEAN, false))))
                .isEqualTo(singletonSequence(new TypedValue(BOOLEAN, false)));

        assertThat(pathResult(
                NullNode.instance,
                path(true, literal(DATE, 1000L))))
                .isEqualTo(singletonSequence(new TypedValue(DATE, 1000L)));
    }

    @Test
    public void testNullLiteral()
    {
        assertThat(pathResult(
                new ArrayNode(JsonNodeFactory.instance),
                path(true, jsonNull())))
                .isEqualTo(singletonSequence(NullNode.instance));
    }

    @Test
    public void testContextVariable()
    {
        JsonNode input = new ArrayNode(JsonNodeFactory.instance, ImmutableList.of(BooleanNode.TRUE, BooleanNode.FALSE));
        assertThat(pathResult(
                input,
                path(true, contextVariable())))
                .isEqualTo(singletonSequence(input));
    }

    @Test
    public void testNamedVariable()
    {
        assertThat(pathResult(
                new ArrayNode(JsonNodeFactory.instance),
                path(true, variable("tinyint_parameter"))))
                .isEqualTo(singletonSequence(new TypedValue(TINYINT, 1L)));

        assertThat(pathResult(
                new ArrayNode(JsonNodeFactory.instance),
                path(true, variable("null_parameter"))))
                .isEqualTo(singletonSequence(NullNode.instance));

        // variables of type IrNamedValueVariable can only take SQL values or JSON null. other JSON objects are handled by IrNamedJsonVariable
        assertThatThrownBy(() -> evaluate(
                new ArrayNode(JsonNodeFactory.instance),
                path(true, variable("json_object_parameter"))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("expected SQL value or JSON null, got non-null JSON");
    }

    @Test
    public void testNamedJsonVariable()
    {
        assertThat(pathResult(
                new ArrayNode(JsonNodeFactory.instance),
                path(true, jsonVariable("null_parameter"))))
                .isEqualTo(singletonSequence(NullNode.instance));

        assertThat(pathResult(
                new ArrayNode(JsonNodeFactory.instance),
                path(true, jsonVariable("json_object_parameter"))))
                .isEqualTo(singletonSequence(new ObjectNode(JsonNodeFactory.instance, ImmutableMap.of("key1", TextNode.valueOf("bound_value"), "key2", NullNode.instance))));

        // variables of type IrNamedJsonVariable can only take JSON objects. SQL values are handled by IrNamedValueVariable
        assertThatThrownBy(() -> evaluate(
                new ArrayNode(JsonNodeFactory.instance),
                path(true, jsonVariable("tinyint_parameter"))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("expected JSON, got SQL value");
    }

    @Test
    public void testAbsMethod()
    {
        assertThat(pathResult(
                IntNode.valueOf(-5),
                path(true, abs(contextVariable()))))
                .isEqualTo(singletonSequence(new TypedValue(INTEGER, 5L)));

        assertThat(pathResult(
                IntNode.valueOf(-5),
                path(true, abs(variable("short_decimal_parameter")))))
                .isEqualTo(singletonSequence(new TypedValue(createDecimalType(3, 1), 123L)));

        assertThat(pathResult(
                IntNode.valueOf(-5),
                path(true, abs(jsonVariable("json_number_parameter")))))
                .isEqualTo(singletonSequence(new TypedValue(INTEGER, 6L)));

        // multiple inputs
        assertThat(pathResult(
                new ArrayNode(JsonNodeFactory.instance, ImmutableList.of(DoubleNode.valueOf(-1e0), IntNode.valueOf(2), ShortNode.valueOf((short) -3))),
                path(true, abs(wildcardArrayAccessor(contextVariable())))))
                .isEqualTo(sequence(new TypedValue(DOUBLE, 1e0), new TypedValue(INTEGER, 2L), new TypedValue(SMALLINT, 3L)));

        // multiple inputs -- array is automatically unwrapped in lax mode
        assertThat(pathResult(
                new ArrayNode(JsonNodeFactory.instance, ImmutableList.of(DoubleNode.valueOf(-1e0), IntNode.valueOf(2), ShortNode.valueOf((short) -3))),
                path(true, abs(contextVariable()))))
                .isEqualTo(sequence(new TypedValue(DOUBLE, 1e0), new TypedValue(INTEGER, 2L), new TypedValue(SMALLINT, 3L)));

        // overflow
        assertThatThrownBy(() -> evaluate(
                IntNode.valueOf(-5),
                path(true, abs(literal(TINYINT, -128L)))))
                .isInstanceOf(PathEvaluationError.class);

        // type mismatch
        assertThatThrownBy(() -> evaluate(
                IntNode.valueOf(-5),
                path(true, abs(jsonVariable("null_parameter")))))
                .isInstanceOf(PathEvaluationError.class)
                .hasMessage("path evaluation failed: invalid item type. Expected: NUMBER, actual: NULL");
    }

    @Test
    public void testArithmeticBinary()
    {
        assertThat(pathResult(
                IntNode.valueOf(-5),
                path(true, add(contextVariable(), variable("short_decimal_parameter")))))
                .isEqualTo(singletonSequence(new TypedValue(createDecimalType(12, 1), -173L)));

        assertThat(pathResult(
                IntNode.valueOf(-5),
                path(true, subtract(literal(DOUBLE, 0e0), variable("short_decimal_parameter")))))
                .isEqualTo(singletonSequence(new TypedValue(DOUBLE, 12.3e0)));

        assertThat(pathResult(
                IntNode.valueOf(-5),
                path(true, multiply(jsonVariable("json_number_parameter"), literal(BIGINT, 3L)))))
                .isEqualTo(singletonSequence(new TypedValue(BIGINT, -18L)));

        // division by 0
        assertThatThrownBy(() -> evaluate(
                IntNode.valueOf(-5),
                path(true, divide(jsonVariable("json_number_parameter"), literal(BIGINT, 0L)))))
                .isInstanceOf(PathEvaluationError.class);

        // type mismatch
        assertThatThrownBy(() -> evaluate(
                IntNode.valueOf(-5),
                path(true, modulus(jsonVariable("json_number_parameter"), literal(BOOLEAN, true)))))
                .isInstanceOf(PathEvaluationError.class)
                .hasMessage("path evaluation failed: invalid operand types to MODULUS operator (integer, boolean)");

        // left operand is not singleton
        assertThatThrownBy(() -> evaluate(
                IntNode.valueOf(-5),
                path(true, add(wildcardArrayAccessor(jsonVariable("json_array_parameter")), literal(BIGINT, 0L)))))
                .isInstanceOf(PathEvaluationError.class)
                .hasMessage("path evaluation failed: arithmetic binary expression requires singleton operands");

        // array is automatically unwrapped in lax mode
        assertThat(pathResult(
                new ArrayNode(JsonNodeFactory.instance, ImmutableList.of(IntNode.valueOf(-5))),
                path(true, multiply(contextVariable(), literal(BIGINT, 3L)))))
                .isEqualTo(singletonSequence(new TypedValue(BIGINT, -15L)));
    }

    @Test
    public void testArithmeticUnary()
    {
        assertThat(pathResult(
                IntNode.valueOf(-5),
                path(true, plus(contextVariable()))))
                .isEqualTo(singletonSequence(new TypedValue(INTEGER, -5L)));

        assertThat(pathResult(
                IntNode.valueOf(-5),
                path(true, minus(variable("short_decimal_parameter")))))
                .isEqualTo(singletonSequence(new TypedValue(createDecimalType(3, 1), 123L)));

        assertThat(pathResult(
                IntNode.valueOf(-5),
                path(true, plus(jsonVariable("json_number_parameter")))))
                .isEqualTo(singletonSequence(new TypedValue(INTEGER, -6L)));

        // multiple inputs
        assertThat(pathResult(
                new ArrayNode(JsonNodeFactory.instance, ImmutableList.of(DoubleNode.valueOf(-1e0), IntNode.valueOf(2), ShortNode.valueOf((short) -3))),
                path(true, minus(wildcardArrayAccessor(contextVariable())))))
                .isEqualTo(sequence(new TypedValue(DOUBLE, 1e0), new TypedValue(INTEGER, -2L), new TypedValue(SMALLINT, 3L)));

        // multiple inputs -- array is automatically unwrapped in lax mode
        assertThat(pathResult(
                new ArrayNode(JsonNodeFactory.instance, ImmutableList.of(DoubleNode.valueOf(-1e0), IntNode.valueOf(2), ShortNode.valueOf((short) -3))),
                path(true, minus(contextVariable()))))
                .isEqualTo(sequence(new TypedValue(DOUBLE, 1e0), new TypedValue(INTEGER, -2L), new TypedValue(SMALLINT, 3L)));

        // overflow
        assertThatThrownBy(() -> evaluate(
                IntNode.valueOf(-5),
                path(true, minus(literal(TINYINT, -128L)))))
                .isInstanceOf(PathEvaluationError.class);

        // type mismatch
        assertThatThrownBy(() -> evaluate(
                IntNode.valueOf(-5),
                path(true, plus(jsonVariable("null_parameter")))))
                .isInstanceOf(PathEvaluationError.class)
                .hasMessage("path evaluation failed: invalid item type. Expected: NUMBER, actual: NULL");
    }

    @Test
    public void testArrayAccessor()
    {
        // wildcard accessor
        assertThat(pathResult(
                new ArrayNode(JsonNodeFactory.instance, ImmutableList.of(DoubleNode.valueOf(-1e0), BooleanNode.TRUE, TextNode.valueOf("some_text"))),
                path(true, wildcardArrayAccessor(contextVariable()))))
                .isEqualTo(sequence(DoubleNode.valueOf(-1e0), BooleanNode.TRUE, TextNode.valueOf("some_text")));

        assertThat(pathResult(
                IntNode.valueOf(-5),
                path(true, wildcardArrayAccessor(jsonVariable("json_array_parameter")))))
                .isEqualTo(sequence(TextNode.valueOf("element"), DoubleNode.valueOf(7e0), NullNode.instance));

        // single element subscript
        assertThat(pathResult(
                IntNode.valueOf(-5),
                path(true, arrayAccessor(jsonVariable("json_array_parameter"), at(literal(DOUBLE, 0e0))))))
                .isEqualTo(singletonSequence(TextNode.valueOf("element")));

        // range subscript
        assertThat(pathResult(
                IntNode.valueOf(-5),
                path(true, arrayAccessor(jsonVariable("json_array_parameter"), range(literal(DOUBLE, 0e0), literal(INTEGER, 1L))))))
                .isEqualTo(sequence(TextNode.valueOf("element"), DoubleNode.valueOf(7e0)));

        // multiple overlapping subscripts
        assertThat(pathResult(
                new ArrayNode(JsonNodeFactory.instance, ImmutableList.of(TextNode.valueOf("first"), TextNode.valueOf("second"), TextNode.valueOf("third"), TextNode.valueOf("fourth"), TextNode.valueOf("fifth"))),
                path(true, arrayAccessor(
                        contextVariable(),
                        range(literal(INTEGER, 3L), literal(INTEGER, 4L)),
                        range(literal(INTEGER, 1L), literal(INTEGER, 2L)),
                        at(literal(INTEGER, 0L))))))
                .isEqualTo(sequence(TextNode.valueOf("fourth"), TextNode.valueOf("fifth"), TextNode.valueOf("second"), TextNode.valueOf("third"), TextNode.valueOf("first")));

        // multiple input arrays
        assertThat(pathResult(
                new ArrayNode(
                        JsonNodeFactory.instance,
                        ImmutableList.of(
                                new ArrayNode(JsonNodeFactory.instance, ImmutableList.of(TextNode.valueOf("first"), TextNode.valueOf("second"), TextNode.valueOf("third"))),
                                new ArrayNode(JsonNodeFactory.instance, ImmutableList.of(IntNode.valueOf(1), IntNode.valueOf(2), IntNode.valueOf(3))))),
                path(true, arrayAccessor(wildcardArrayAccessor(contextVariable()), range(literal(INTEGER, 1L), literal(INTEGER, 2L))))))
                .isEqualTo(sequence(TextNode.valueOf("second"), TextNode.valueOf("third"), IntNode.valueOf(2), IntNode.valueOf(3)));

        // usage of last variable
        assertThat(pathResult(
                IntNode.valueOf(-5),
                path(true, arrayAccessor(jsonVariable("json_array_parameter"), range(literal(DOUBLE, 1e0), last())))))
                .isEqualTo(sequence(DoubleNode.valueOf(7e0), NullNode.instance));

        // incorrect usage of last variable: no enclosing array
        assertThatThrownBy(() -> evaluate(
                IntNode.valueOf(-5),
                path(true, last())))
                .isInstanceOf(PathEvaluationError.class)
                .hasMessage("path evaluation failed: accessing the last array index with no enclosing array");

        // last variable in nested arrays
        assertThat(pathResult(
                new ArrayNode(
                        JsonNodeFactory.instance,
                        ImmutableList.of(
                                new ArrayNode(JsonNodeFactory.instance, ImmutableList.of(IntNode.valueOf(2), IntNode.valueOf(3), IntNode.valueOf(5))),
                                IntNode.valueOf(7))),
                path(true, multiply(
                        arrayAccessor(arrayAccessor(contextVariable(), at(literal(INTEGER, 0L))), at(last())), // 5
                        arrayAccessor(contextVariable(), at(last())))))) // 7
                .isEqualTo(singletonSequence(new TypedValue(INTEGER, 35L)));

        // subscript out of bounds (lax mode)
        assertThat(pathResult(
                new ArrayNode(JsonNodeFactory.instance, ImmutableList.of(TextNode.valueOf("first"), TextNode.valueOf("second"), TextNode.valueOf("third"), TextNode.valueOf("fourth"), TextNode.valueOf("fifth"))),
                path(true, arrayAccessor(contextVariable(), at(literal(INTEGER, 100L))))))
                .isEqualTo(emptySequence());

        assertThat(pathResult(
                new ArrayNode(JsonNodeFactory.instance, ImmutableList.of(TextNode.valueOf("first"), TextNode.valueOf("second"), TextNode.valueOf("third"), TextNode.valueOf("fourth"), TextNode.valueOf("fifth"))),
                path(true, arrayAccessor(contextVariable(), range(literal(INTEGER, 3L), literal(INTEGER, 100L))))))
                .isEqualTo(sequence(TextNode.valueOf("fourth"), TextNode.valueOf("fifth")));

        // incorrect subscript: from > to (lax mode)
        assertThat(pathResult(
                new ArrayNode(JsonNodeFactory.instance, ImmutableList.of(TextNode.valueOf("first"), TextNode.valueOf("second"), TextNode.valueOf("third"), TextNode.valueOf("fourth"), TextNode.valueOf("fifth"))),
                path(true, arrayAccessor(contextVariable(), range(literal(INTEGER, 3L), literal(INTEGER, 2L))))))
                .isEqualTo(emptySequence());

        // subscript out of bounds (strict mode)
        assertThatThrownBy(() -> evaluate(
                new ArrayNode(JsonNodeFactory.instance, ImmutableList.of(TextNode.valueOf("first"), TextNode.valueOf("second"), TextNode.valueOf("third"), TextNode.valueOf("fourth"), TextNode.valueOf("fifth"))),
                path(false, arrayAccessor(contextVariable(), at(literal(INTEGER, 100L))))))
                .isInstanceOf(PathEvaluationError.class)
                .hasMessage("path evaluation failed: structural error: invalid array subscript: [100, 100] for array of size 5");

        assertThatThrownBy(() -> evaluate(
                new ArrayNode(JsonNodeFactory.instance, ImmutableList.of(TextNode.valueOf("first"), TextNode.valueOf("second"), TextNode.valueOf("third"), TextNode.valueOf("fourth"), TextNode.valueOf("fifth"))),
                path(false, arrayAccessor(contextVariable(), range(literal(INTEGER, 3L), literal(INTEGER, 100L))))))
                .isInstanceOf(PathEvaluationError.class)
                .hasMessage("path evaluation failed: structural error: invalid array subscript: [3, 100] for array of size 5");

        // incorrect subscript: from > to (strict mode)
        assertThatThrownBy(() -> evaluate(
                new ArrayNode(JsonNodeFactory.instance, ImmutableList.of(TextNode.valueOf("first"), TextNode.valueOf("second"), TextNode.valueOf("third"), TextNode.valueOf("fourth"), TextNode.valueOf("fifth"))),
                path(false, arrayAccessor(contextVariable(), range(literal(INTEGER, 3L), literal(INTEGER, 2L))))))
                .isInstanceOf(PathEvaluationError.class)
                .hasMessage("path evaluation failed: structural error: invalid array subscript: [3, 2] for array of size 5");

        // type mismatch (lax mode) -> the value is wrapped in a singleton array
        assertThat(pathResult(
                IntNode.valueOf(-5),
                path(true, arrayAccessor(contextVariable(), at(literal(INTEGER, 0L))))))
                .isEqualTo(singletonSequence(IntNode.valueOf(-5)));

        // type mismatch (strict mode)
        assertThatThrownBy(() -> evaluate(
                IntNode.valueOf(-5),
                path(false, arrayAccessor(contextVariable(), at(literal(INTEGER, 0L))))))
                .isInstanceOf(PathEvaluationError.class)
                .hasMessage("path evaluation failed: invalid item type. Expected: ARRAY, actual: NUMBER");
    }

    @Test
    public void testCeilingMethod()
    {
        assertThat(pathResult(
                IntNode.valueOf(-5),
                path(true, ceiling(contextVariable()))))
                .isEqualTo(singletonSequence(new TypedValue(INTEGER, -5L)));

        assertThat(pathResult(
                IntNode.valueOf(-5),
                path(true, ceiling(variable("short_decimal_parameter")))))
                .isEqualTo(singletonSequence(new TypedValue(createDecimalType(3, 0), -12L)));

        assertThat(pathResult(
                IntNode.valueOf(-5),
                path(true, ceiling(jsonVariable("json_number_parameter")))))
                .isEqualTo(singletonSequence(new TypedValue(INTEGER, -6L)));

        // multiple inputs
        assertThat(pathResult(
                new ArrayNode(JsonNodeFactory.instance, ImmutableList.of(DoubleNode.valueOf(1.5e0), IntNode.valueOf(2), DecimalNode.valueOf(BigDecimal.valueOf(-15, 1)))),
                path(true, ceiling(wildcardArrayAccessor(contextVariable())))))
                .isEqualTo(sequence(new TypedValue(DOUBLE, 2e0), new TypedValue(INTEGER, 2L), new TypedValue(createDecimalType(2, 0), -1L)));

        // multiple inputs -- array is automatically unwrapped in lax mode
        assertThat(pathResult(
                new ArrayNode(JsonNodeFactory.instance, ImmutableList.of(DoubleNode.valueOf(1.5e0), IntNode.valueOf(2), DecimalNode.valueOf(BigDecimal.valueOf(-15, 1)))),
                path(true, ceiling(contextVariable()))))
                .isEqualTo(sequence(new TypedValue(DOUBLE, 2e0), new TypedValue(INTEGER, 2L), new TypedValue(createDecimalType(2, 0), -1L)));

        // type mismatch
        assertThatThrownBy(() -> evaluate(
                IntNode.valueOf(-5),
                path(true, ceiling(jsonVariable("null_parameter")))))
                .isInstanceOf(PathEvaluationError.class)
                .hasMessage("path evaluation failed: invalid item type. Expected: NUMBER, actual: NULL");
    }

    @Test
    public void testDoubleMethod()
    {
        assertThat(pathResult(
                IntNode.valueOf(-5),
                path(true, toDouble(contextVariable()))))
                .isEqualTo(singletonSequence(new TypedValue(DOUBLE, -5e0)));

        assertThat(pathResult(
                IntNode.valueOf(-5),
                path(true, toDouble(variable("short_decimal_parameter")))))
                .isEqualTo(singletonSequence(new TypedValue(DOUBLE, -12.3e0)));

        assertThat(pathResult(
                IntNode.valueOf(-5),
                path(true, toDouble(jsonVariable("json_number_parameter")))))
                .isEqualTo(singletonSequence(new TypedValue(DOUBLE, -6e0)));

        assertThat(pathResult(
                TextNode.valueOf("123"),
                path(true, toDouble(contextVariable()))))
                .isEqualTo(singletonSequence(new TypedValue(DOUBLE, 123e0)));

        assertThat(pathResult(
                TextNode.valueOf("-12.3e5"),
                path(true, toDouble(contextVariable()))))
                .isEqualTo(singletonSequence(new TypedValue(DOUBLE, -12.3e5)));

        // multiple inputs
        assertThat(pathResult(
                new ArrayNode(JsonNodeFactory.instance, ImmutableList.of(DoubleNode.valueOf(1.5e0), IntNode.valueOf(2), DecimalNode.valueOf(BigDecimal.valueOf(-15, 1)))),
                path(true, toDouble(wildcardArrayAccessor(contextVariable())))))
                .isEqualTo(sequence(new TypedValue(DOUBLE, 1.5e0), new TypedValue(DOUBLE, 2e0), new TypedValue(DOUBLE, -1.5e0)));

        // multiple inputs -- array is automatically unwrapped in lax mode
        assertThat(pathResult(
                new ArrayNode(JsonNodeFactory.instance, ImmutableList.of(DoubleNode.valueOf(1.5e0), IntNode.valueOf(2), DecimalNode.valueOf(BigDecimal.valueOf(-15, 1)))),
                path(true, toDouble(contextVariable()))))
                .isEqualTo(sequence(new TypedValue(DOUBLE, 1.5e0), new TypedValue(DOUBLE, 2e0), new TypedValue(DOUBLE, -1.5e0)));

        // type mismatch
        assertThatThrownBy(() -> evaluate(
                IntNode.valueOf(-5),
                path(true, toDouble(jsonVariable("null_parameter")))))
                .isInstanceOf(PathEvaluationError.class)
                .hasMessage("path evaluation failed: invalid item type. Expected: NUMBER or TEXT, actual: NULL");
    }

    @Test
    public void testFloorMethod()
    {
        assertThat(pathResult(
                IntNode.valueOf(-5),
                path(true, floor(contextVariable()))))
                .isEqualTo(singletonSequence(new TypedValue(INTEGER, -5L)));

        assertThat(pathResult(
                IntNode.valueOf(-5),
                path(true, floor(variable("short_decimal_parameter")))))
                .isEqualTo(singletonSequence(new TypedValue(createDecimalType(3, 0), -13L)));

        assertThat(pathResult(
                IntNode.valueOf(-5),
                path(true, floor(jsonVariable("json_number_parameter")))))
                .isEqualTo(singletonSequence(new TypedValue(INTEGER, -6L)));

        // multiple inputs
        assertThat(pathResult(
                new ArrayNode(JsonNodeFactory.instance, ImmutableList.of(DoubleNode.valueOf(1.5e0), IntNode.valueOf(2), DecimalNode.valueOf(BigDecimal.valueOf(-15, 1)))),
                path(true, floor(wildcardArrayAccessor(contextVariable())))))
                .isEqualTo(sequence(new TypedValue(DOUBLE, 1e0), new TypedValue(INTEGER, 2L), new TypedValue(createDecimalType(2, 0), -2L)));

        // multiple inputs -- array is automatically unwrapped in lax mode
        assertThat(pathResult(
                new ArrayNode(JsonNodeFactory.instance, ImmutableList.of(DoubleNode.valueOf(1.5e0), IntNode.valueOf(2), DecimalNode.valueOf(BigDecimal.valueOf(-15, 1)))),
                path(true, floor(contextVariable()))))
                .isEqualTo(sequence(new TypedValue(DOUBLE, 1e0), new TypedValue(INTEGER, 2L), new TypedValue(createDecimalType(2, 0), -2L)));

        // type mismatch
        assertThatThrownBy(() -> evaluate(
                IntNode.valueOf(-5),
                path(true, floor(jsonVariable("null_parameter")))))
                .isInstanceOf(PathEvaluationError.class)
                .hasMessage("path evaluation failed: invalid item type. Expected: NUMBER, actual: NULL");
    }

    @Test
    public void testKeyValueMethod()
    {
        assertThat(pathResult(
                new ObjectNode(JsonNodeFactory.instance, ImmutableMap.of("key1", TextNode.valueOf("bound_value"), "key2", NullNode.instance)),
                path(true, keyValue(contextVariable()))))
                .isEqualTo(sequence(
                        new ObjectNode(JsonNodeFactory.instance, ImmutableMap.of(
                                "name", TextNode.valueOf("key1"),
                                "value", TextNode.valueOf("bound_value"),
                                "id", IntNode.valueOf(0))),
                        new ObjectNode(JsonNodeFactory.instance, ImmutableMap.of(
                                "name", TextNode.valueOf("key2"),
                                "value", NullNode.instance,
                                "id", IntNode.valueOf(0)))));

        assertThat(pathResult(
                IntNode.valueOf(-5),
                path(true, keyValue(jsonVariable("json_object_parameter")))))
                .isEqualTo(sequence(
                        new ObjectNode(JsonNodeFactory.instance, ImmutableMap.of(
                                "name", TextNode.valueOf("key1"),
                                "value", TextNode.valueOf("bound_value"),
                                "id", IntNode.valueOf(0))),
                        new ObjectNode(JsonNodeFactory.instance, ImmutableMap.of(
                                "name", TextNode.valueOf("key2"),
                                "value", NullNode.instance,
                                "id", IntNode.valueOf(0)))));

        // multiple input objects
        assertThat(pathResult(
                new ArrayNode(
                        JsonNodeFactory.instance,
                        ImmutableList.of(
                                new ObjectNode(JsonNodeFactory.instance, ImmutableMap.of("key1", TextNode.valueOf("first"), "key2", BooleanNode.TRUE)),
                                new ObjectNode(JsonNodeFactory.instance, ImmutableMap.of("key3", IntNode.valueOf(1), "key4", NullNode.instance)))),
                path(true, keyValue(wildcardArrayAccessor(contextVariable())))))
                .isEqualTo(sequence(
                        new ObjectNode(JsonNodeFactory.instance, ImmutableMap.of(
                                "name", TextNode.valueOf("key1"),
                                "value", TextNode.valueOf("first"),
                                "id", IntNode.valueOf(0))),
                        new ObjectNode(JsonNodeFactory.instance, ImmutableMap.of(
                                "name", TextNode.valueOf("key2"),
                                "value", BooleanNode.TRUE,
                                "id", IntNode.valueOf(0))),
                        new ObjectNode(JsonNodeFactory.instance, ImmutableMap.of(
                                "name", TextNode.valueOf("key3"),
                                "value", IntNode.valueOf(1),
                                "id", IntNode.valueOf(1))),
                        new ObjectNode(JsonNodeFactory.instance, ImmutableMap.of(
                                "name", TextNode.valueOf("key4"),
                                "value", NullNode.instance,
                                "id", IntNode.valueOf(1)))));

        // multiple objects -- array is automatically unwrapped in lax mode
        assertThat(pathResult(
                new ArrayNode(
                        JsonNodeFactory.instance,
                        ImmutableList.of(
                                new ObjectNode(JsonNodeFactory.instance, ImmutableMap.of("key1", TextNode.valueOf("first"), "key2", BooleanNode.TRUE)),
                                new ObjectNode(JsonNodeFactory.instance, ImmutableMap.of("key3", IntNode.valueOf(1), "key4", NullNode.instance)))),
                path(true, keyValue(wildcardArrayAccessor(contextVariable())))))
                .isEqualTo(sequence(
                        new ObjectNode(JsonNodeFactory.instance, ImmutableMap.of(
                                "name", TextNode.valueOf("key1"),
                                "value", TextNode.valueOf("first"),
                                "id", IntNode.valueOf(0))),
                        new ObjectNode(JsonNodeFactory.instance, ImmutableMap.of(
                                "name", TextNode.valueOf("key2"),
                                "value", BooleanNode.TRUE,
                                "id", IntNode.valueOf(0))),
                        new ObjectNode(JsonNodeFactory.instance, ImmutableMap.of(
                                "name", TextNode.valueOf("key3"),
                                "value", IntNode.valueOf(1),
                                "id", IntNode.valueOf(1))),
                        new ObjectNode(JsonNodeFactory.instance, ImmutableMap.of(
                                "name", TextNode.valueOf("key4"),
                                "value", NullNode.instance,
                                "id", IntNode.valueOf(1)))));

        // type mismatch
        assertThatThrownBy(() -> evaluate(
                IntNode.valueOf(-5),
                path(true, keyValue(jsonVariable("null_parameter")))))
                .isInstanceOf(PathEvaluationError.class)
                .hasMessage("path evaluation failed: invalid item type. Expected: OBJECT, actual: NULL");
    }

    @Test
    public void testMemberAccessor()
    {
        // wildcard accessor
        assertThat(pathResult(
                new ObjectNode(JsonNodeFactory.instance, ImmutableMap.of("key1", TextNode.valueOf("bound_value"), "key2", NullNode.instance)),
                path(true, wildcardMemberAccessor(contextVariable()))))
                .isEqualTo(sequence(TextNode.valueOf("bound_value"), NullNode.instance));

        assertThat(pathResult(
                IntNode.valueOf(-5),
                path(true, memberAccessor(jsonVariable("json_object_parameter"), "key1"))))
                .isEqualTo(singletonSequence(TextNode.valueOf("bound_value")));

        // multiple input objects
        assertThat(pathResult(
                new ArrayNode(
                        JsonNodeFactory.instance,
                        ImmutableList.of(
                                new ObjectNode(JsonNodeFactory.instance, ImmutableMap.of("key1", TextNode.valueOf("first"), "key2", BooleanNode.TRUE)),
                                new ObjectNode(JsonNodeFactory.instance, ImmutableMap.of("key1", IntNode.valueOf(1), "key2", NullNode.instance)))),
                path(true, memberAccessor(wildcardArrayAccessor(contextVariable()), "key2"))))
                .isEqualTo(sequence(BooleanNode.TRUE, NullNode.instance));

        // multiple input objects -- array is automatically unwrapped in lax mode
        assertThat(pathResult(
                new ArrayNode(
                        JsonNodeFactory.instance,
                        ImmutableList.of(
                                new ObjectNode(JsonNodeFactory.instance, ImmutableMap.of("key1", TextNode.valueOf("first"), "key2", BooleanNode.TRUE)),
                                new ObjectNode(JsonNodeFactory.instance, ImmutableMap.of("key1", IntNode.valueOf(1), "key2", NullNode.instance)))),
                path(true, memberAccessor(contextVariable(), "key2"))))
                .isEqualTo(sequence(BooleanNode.TRUE, NullNode.instance));

        // key not found -- structural error is suppressed in lax mode
        assertThat(pathResult(
                new ObjectNode(JsonNodeFactory.instance, ImmutableMap.of("key1", TextNode.valueOf("bound_value"), "key2", NullNode.instance)),
                path(true, memberAccessor(contextVariable(), "wrong_key"))))
                .isEqualTo(emptySequence());

        // key not found -- strict mode
        assertThatThrownBy(() -> evaluate(
                new ObjectNode(JsonNodeFactory.instance, ImmutableMap.of("key1", TextNode.valueOf("bound_value"), "key2", NullNode.instance)),
                path(false, memberAccessor(contextVariable(), "wrong_key"))))
                .isInstanceOf(PathEvaluationError.class)
                .hasMessage("path evaluation failed: structural error: missing member 'wrong_key' in JSON object");

        // multiple input objects, key not found in one of them -- lax mode
        assertThat(pathResult(
                new ArrayNode(
                        JsonNodeFactory.instance,
                        ImmutableList.of(
                                new ObjectNode(JsonNodeFactory.instance, ImmutableMap.of("key1", TextNode.valueOf("first"), "key2", BooleanNode.TRUE)),
                                new ObjectNode(JsonNodeFactory.instance, ImmutableMap.of("key3", IntNode.valueOf(1), "key4", NullNode.instance)))),
                path(true, memberAccessor(wildcardArrayAccessor(contextVariable()), "key2"))))
                .isEqualTo(singletonSequence(BooleanNode.TRUE));

        // multiple input objects, key not found in one of them -- strict mode
        assertThatThrownBy(() -> evaluate(
                new ArrayNode(
                        JsonNodeFactory.instance,
                        ImmutableList.of(
                                new ObjectNode(JsonNodeFactory.instance, ImmutableMap.of("key1", TextNode.valueOf("first"), "key2", BooleanNode.TRUE)),
                                new ObjectNode(JsonNodeFactory.instance, ImmutableMap.of("key3", IntNode.valueOf(1), "key4", NullNode.instance)))),
                path(false, memberAccessor(wildcardArrayAccessor(contextVariable()), "key2"))))
                .isInstanceOf(PathEvaluationError.class)
                .hasMessage("path evaluation failed: structural error: missing member 'key2' in JSON object");

        // type mismatch
        assertThatThrownBy(() -> evaluate(
                IntNode.valueOf(-5),
                path(true, keyValue(jsonVariable("null_parameter")))))
                .isInstanceOf(PathEvaluationError.class)
                .hasMessage("path evaluation failed: invalid item type. Expected: OBJECT, actual: NULL");
    }

    @Test
    public void testSizeMethod()
    {
        assertThat(pathResult(
                IntNode.valueOf(-5),
                path(true, size(contextVariable()))))
                .isEqualTo(singletonSequence(new TypedValue(INTEGER, 1L)));

        assertThat(pathResult(
                IntNode.valueOf(-5),
                path(true, size(variable("short_decimal_parameter")))))
                .isEqualTo(singletonSequence(new TypedValue(INTEGER, 1L)));

        assertThat(pathResult(
                IntNode.valueOf(-5),
                path(true, size(jsonVariable("json_boolean_parameter")))))
                .isEqualTo(singletonSequence(new TypedValue(INTEGER, 1L)));

        assertThat(pathResult(
                NullNode.instance,
                path(true, size(contextVariable()))))
                .isEqualTo(singletonSequence(new TypedValue(INTEGER, 1L)));

        assertThat(pathResult(
                IntNode.valueOf(-5),
                path(true, size(jsonVariable("json_object_parameter")))))
                .isEqualTo(singletonSequence(new TypedValue(INTEGER, 1L)));

        assertThat(pathResult(
                IntNode.valueOf(-5),
                path(true, size(jsonVariable("json_array_parameter")))))
                .isEqualTo(singletonSequence(new TypedValue(INTEGER, 3L)));

        // multiple inputs
        assertThat(pathResult(
                new ArrayNode(JsonNodeFactory.instance, ImmutableList.of(DoubleNode.valueOf(1.5e0), new ArrayNode(JsonNodeFactory.instance, ImmutableList.of(BooleanNode.TRUE, BooleanNode.FALSE)))),
                path(true, size(wildcardArrayAccessor(contextVariable())))))
                .isEqualTo(sequence(new TypedValue(INTEGER, 1L), new TypedValue(INTEGER, 2L)));

        // type mismatch
        assertThatThrownBy(() -> evaluate(
                IntNode.valueOf(-5),
                path(false, size(contextVariable()))))
                .isInstanceOf(PathEvaluationError.class)
                .hasMessage("path evaluation failed: invalid item type. Expected: ARRAY, actual: NUMBER");
    }

    @Test
    public void testTypeMethod()
    {
        assertThat(pathResult(
                IntNode.valueOf(-5),
                path(true, type(contextVariable()))))
                .isEqualTo(singletonSequence(new TypedValue(createVarcharType(27), utf8Slice("number"))));

        assertThat(pathResult(
                IntNode.valueOf(-5),
                path(true, type(variable("string_parameter")))))
                .isEqualTo(singletonSequence(new TypedValue(createVarcharType(27), utf8Slice("string"))));

        assertThat(pathResult(
                IntNode.valueOf(-5),
                path(true, type(jsonVariable("json_boolean_parameter")))))
                .isEqualTo(singletonSequence(new TypedValue(createVarcharType(27), utf8Slice("boolean"))));

        assertThat(pathResult(
                IntNode.valueOf(-5),
                path(true, type(jsonVariable("json_array_parameter")))))
                .isEqualTo(singletonSequence(new TypedValue(createVarcharType(27), utf8Slice("array"))));

        assertThat(pathResult(
                IntNode.valueOf(-5),
                path(true, type(jsonVariable("json_object_parameter")))))
                .isEqualTo(singletonSequence(new TypedValue(createVarcharType(27), utf8Slice("object"))));

        assertThat(pathResult(
                NullNode.instance,
                path(true, type(contextVariable()))))
                .isEqualTo(singletonSequence(new TypedValue(createVarcharType(27), utf8Slice("null"))));

        assertThat(pathResult(
                IntNode.valueOf(-5),
                path(true, type(variable("date_parameter")))))
                .isEqualTo(singletonSequence(new TypedValue(createVarcharType(27), utf8Slice("date"))));

        // multiple inputs
        assertThat(pathResult(
                new ArrayNode(JsonNodeFactory.instance, ImmutableList.of(DoubleNode.valueOf(1.5e0), new ArrayNode(JsonNodeFactory.instance, ImmutableList.of(BooleanNode.TRUE, BooleanNode.FALSE)))),
                path(true, type(wildcardArrayAccessor(contextVariable())))))
                .isEqualTo(sequence(new TypedValue(createVarcharType(27), utf8Slice("number")), new TypedValue(createVarcharType(27), utf8Slice("array"))));
    }

    // JSON PREDICATE
    @Test
    public void testComparisonPredicate()
    {
        assertThat(predicateResult(
                DoubleNode.valueOf(1e0),
                LongNode.valueOf(1L),
                true,
                equal(contextVariable(), currentItem())))
                .isEqualTo(TRUE);

        assertThat(predicateResult(
                DoubleNode.valueOf(1e0),
                LongNode.valueOf(1L),
                true,
                notEqual(jsonVariable("json_number_parameter"), variable("double_parameter"))))
                .isEqualTo(TRUE);

        assertThat(predicateResult(
                DoubleNode.valueOf(1e0),
                LongNode.valueOf(1L),
                true,
                lessThan(literal(BOOLEAN, true), literal(BOOLEAN, false))))
                .isEqualTo(FALSE);

        assertThat(predicateResult(
                DoubleNode.valueOf(1e0),
                LongNode.valueOf(1L),
                true,
                greaterThan(literal(VARCHAR, utf8Slice("xyz")), literal(VARCHAR, utf8Slice("abc")))))
                .isEqualTo(TRUE);

        assertThat(predicateResult(
                DoubleNode.valueOf(1e0),
                LongNode.valueOf(1L),
                true,
                lessThanOrEqual(literal(DATE, 0L), variable("date_parameter"))))
                .isEqualTo(TRUE);

        assertThat(predicateResult(
                DoubleNode.valueOf(1e0),
                LongNode.valueOf(1L),
                true,
                greaterThanOrEqual(literal(BIGINT, 1L), literal(BIGINT, 2L))))
                .isEqualTo(FALSE);

        // uncomparable items -> result unknown
        assertThat(predicateResult(
                DoubleNode.valueOf(1e0),
                TextNode.valueOf("abc"),
                false,
                lessThan(contextVariable(), currentItem())))
                .isEqualTo(null);

        // nulls can be compared with every item
        assertThat(predicateResult(
                DoubleNode.valueOf(1e0),
                LongNode.valueOf(1L),
                true,
                equal(jsonNull(), jsonNull())))
                .isEqualTo(TRUE);

        assertThat(predicateResult(
                DoubleNode.valueOf(1e0),
                LongNode.valueOf(1L),
                true,
                lessThan(jsonNull(), currentItem())))
                .isEqualTo(FALSE);

        assertThat(predicateResult(
                DoubleNode.valueOf(1e0),
                LongNode.valueOf(1L),
                true,
                notEqual(jsonNull(), jsonVariable("json_object_parameter"))))
                .isEqualTo(TRUE);

        // array / object can only be compared with null. otherwise the result is unknown
        assertThat(predicateResult(
                DoubleNode.valueOf(1e0),
                LongNode.valueOf(1L),
                true,
                notEqual(jsonVariable("json_object_parameter"), jsonVariable("json_object_parameter"))))
                .isEqualTo(null);

        assertThat(predicateResult(
                DoubleNode.valueOf(1e0),
                LongNode.valueOf(1L),
                true,
                notEqual(jsonVariable("json_array_parameter"), jsonVariable("json_object_parameter"))))
                .isEqualTo(null);

        // array is automatically unwrapped in lax mode
        assertThat(predicateResult(
                new ArrayNode(JsonNodeFactory.instance, ImmutableList.of(IntNode.valueOf(1))),
                LongNode.valueOf(1L),
                true,
                equal(contextVariable(), literal(BIGINT, 1L))))
                .isEqualTo(TRUE);

        // array is not unwrapped in strict mode
        assertThat(predicateResult(
                new ArrayNode(JsonNodeFactory.instance, ImmutableList.of(IntNode.valueOf(1))),
                LongNode.valueOf(1L),
                false,
                equal(contextVariable(), literal(BIGINT, 1L))))
                .isEqualTo(null);

        // multiple items - first success or failure in lax mode
        assertThat(predicateResult(
                new ArrayNode(JsonNodeFactory.instance, ImmutableList.of(IntNode.valueOf(1), IntNode.valueOf(2))),
                new ArrayNode(JsonNodeFactory.instance, ImmutableList.of(IntNode.valueOf(3), BooleanNode.TRUE)),
                true,
                lessThan(contextVariable(), currentItem())))
                .isEqualTo(TRUE);

        assertThat(predicateResult(
                new ArrayNode(JsonNodeFactory.instance, ImmutableList.of(BooleanNode.TRUE, IntNode.valueOf(2))),
                new ArrayNode(JsonNodeFactory.instance, ImmutableList.of(IntNode.valueOf(3), IntNode.valueOf(1))),
                true,
                lessThan(contextVariable(), currentItem())))
                .isEqualTo(null);

        // null based equal in lax mode
        assertThat(predicateResult(
                new ArrayNode(JsonNodeFactory.instance, ImmutableList.of(IntNode.valueOf(1), NullNode.instance)),
                new ArrayNode(JsonNodeFactory.instance, ImmutableList.of(IntNode.valueOf(3), NullNode.instance)),
                true,
                equal(contextVariable(), currentItem())))
                .isEqualTo(TRUE);

        // null based not equal in lax mode
        assertThat(predicateResult(
                new ArrayNode(JsonNodeFactory.instance, ImmutableList.of(IntNode.valueOf(1), NullNode.instance)),
                new ArrayNode(JsonNodeFactory.instance, ImmutableList.of(IntNode.valueOf(3), BooleanNode.TRUE)),
                true,
                notEqual(contextVariable(), currentItem())))
                .isEqualTo(TRUE);

        // multiple items - fail if any comparison returns error in strict mode
        assertThat(predicateResult(
                new ArrayNode(JsonNodeFactory.instance, ImmutableList.of(IntNode.valueOf(1), IntNode.valueOf(2))),
                new ArrayNode(JsonNodeFactory.instance, ImmutableList.of(IntNode.valueOf(3), BooleanNode.TRUE)),
                false,
                lessThan(contextVariable(), currentItem())))
                .isEqualTo(null);

        // error while evaluating nested path (floor method called on a text value) -> result unknown
        assertThat(predicateResult(
                DoubleNode.valueOf(1e0),
                TextNode.valueOf("abc"),
                true,
                lessThan(contextVariable(), floor(currentItem()))))
                .isEqualTo(null);

        // left operand returns empty sequence -> result false
        assertThat(predicateResult(
                new ArrayNode(JsonNodeFactory.instance, ImmutableList.of(IntNode.valueOf(1), IntNode.valueOf(2))),
                new ArrayNode(JsonNodeFactory.instance, ImmutableList.of(IntNode.valueOf(3), IntNode.valueOf(4))),
                true,
                lessThan(arrayAccessor(contextVariable(), at(literal(BIGINT, 100L))), currentItem())))
                .isEqualTo(false);

        // right operand returns empty sequence -> result false
        assertThat(predicateResult(
                new ArrayNode(JsonNodeFactory.instance, ImmutableList.of(IntNode.valueOf(1), IntNode.valueOf(2))),
                new ArrayNode(JsonNodeFactory.instance, ImmutableList.of(IntNode.valueOf(3), IntNode.valueOf(4))),
                true,
                lessThan(contextVariable(), arrayAccessor(currentItem(), at(literal(BIGINT, 100L))))))
                .isEqualTo(false);
    }

    @Test
    public void testConjunctionPredicate()
    {
        assertThat(predicateResult(
                DoubleNode.valueOf(1e0),
                TextNode.valueOf("abc"),
                true,
                conjunction(
                        equal(literal(BIGINT, 1L), literal(BIGINT, 1L)), // true
                        equal(literal(BIGINT, 2L), literal(BIGINT, 2L))))) // true
                .isEqualTo(TRUE);

        assertThat(predicateResult(
                DoubleNode.valueOf(1e0),
                TextNode.valueOf("abc"),
                true,
                conjunction(
                        equal(literal(BIGINT, 1L), literal(BIGINT, 1L)), // true
                        equal(literal(BIGINT, 2L), literal(BOOLEAN, false))))) // unknown
                .isEqualTo(null);

        assertThat(predicateResult(
                DoubleNode.valueOf(1e0),
                TextNode.valueOf("abc"),
                true,
                conjunction(
                        equal(literal(BIGINT, 1L), literal(BOOLEAN, false)), // unknown
                        equal(literal(BIGINT, 2L), literal(BIGINT, 3L))))) // false
                .isEqualTo(FALSE);
    }

    @Test
    public void testDisjunctionPredicate()
    {
        assertThat(predicateResult(
                DoubleNode.valueOf(1e0),
                TextNode.valueOf("abc"),
                true,
                disjunction(
                        equal(literal(BIGINT, 1L), literal(BIGINT, 2L)), // false
                        equal(literal(BIGINT, 2L), literal(BIGINT, 2L))))) // true
                .isEqualTo(TRUE);

        assertThat(predicateResult(
                DoubleNode.valueOf(1e0),
                TextNode.valueOf("abc"),
                true,
                disjunction(
                        equal(literal(BIGINT, 1L), literal(BIGINT, 2L)), // false
                        equal(literal(BIGINT, 1L), literal(BOOLEAN, false))))) // unknown
                .isEqualTo(null);

        assertThat(predicateResult(
                DoubleNode.valueOf(1e0),
                TextNode.valueOf("abc"),
                true,
                disjunction(
                        equal(literal(BIGINT, 1L), literal(BIGINT, 2L)), // false
                        equal(literal(BIGINT, 2L), literal(BIGINT, 3L))))) // false
                .isEqualTo(FALSE);
    }

    @Test
    public void testExistsPredicate()
    {
        assertThat(predicateResult(
                DoubleNode.valueOf(1e0),
                TextNode.valueOf("abc"),
                true,
                exists(contextVariable())))
                .isEqualTo(TRUE);

        // member accessor with non-existent key returns empty sequence in lax mode
        assertThat(predicateResult(
                DoubleNode.valueOf(1e0),
                TextNode.valueOf("abc"),
                true,
                exists(memberAccessor(jsonVariable("json_object_parameter"), "wrong_key"))))
                .isEqualTo(FALSE);

        // member accessor with non-existent key returns error in strict mode
        assertThat(predicateResult(
                DoubleNode.valueOf(1e0),
                TextNode.valueOf("abc"),
                false,
                exists(memberAccessor(jsonVariable("json_object_parameter"), "wrong_key"))))
                .isEqualTo(null);
    }

    @Test
    public void testIsUnknownPredicate()
    {
        assertThat(predicateResult(
                DoubleNode.valueOf(1e0),
                LongNode.valueOf(1L),
                true,
                isUnknown(equal(literal(BIGINT, 1L), literal(BIGINT, 1L))))) // true
                .isEqualTo(FALSE);

        assertThat(predicateResult(
                DoubleNode.valueOf(1e0),
                LongNode.valueOf(1L),
                true,
                isUnknown(equal(literal(BIGINT, 1L), literal(BIGINT, 2L))))) // false
                .isEqualTo(FALSE);

        assertThat(predicateResult(
                DoubleNode.valueOf(1e0),
                LongNode.valueOf(1L),
                true,
                isUnknown(equal(literal(BIGINT, 1L), literal(BOOLEAN, true))))) // unknown
                .isEqualTo(TRUE);
    }

    @Test
    public void testNegationPredicate()
    {
        assertThat(predicateResult(
                DoubleNode.valueOf(1e0),
                TextNode.valueOf("abc"),
                true,
                negation(equal(literal(BIGINT, 1L), literal(BIGINT, 1L)))))
                .isEqualTo(FALSE);

        assertThat(predicateResult(
                DoubleNode.valueOf(1e0),
                TextNode.valueOf("abc"),
                true,
                negation(notEqual(literal(BIGINT, 1L), literal(BIGINT, 1L)))))
                .isEqualTo(TRUE);

        assertThat(predicateResult(
                DoubleNode.valueOf(1e0),
                TextNode.valueOf("abc"),
                true,
                negation(equal(literal(BIGINT, 1L), literal(BOOLEAN, false)))))
                .isEqualTo(null);
    }

    @Test
    public void testStartsWithPredicate()
    {
        assertThat(predicateResult(
                TextNode.valueOf("abcde"),
                TextNode.valueOf("abc"),
                true,
                startsWith(contextVariable(), currentItem())))
                .isEqualTo(TRUE);

        assertThat(predicateResult(
                TextNode.valueOf("abcde"),
                TextNode.valueOf("abc"),
                true,
                startsWith(jsonVariable("json_text_parameter"), literal(createCharType(4), utf8Slice("JSON")))))
                .isEqualTo(TRUE);

        assertThat(predicateResult(
                TextNode.valueOf("abcde"),
                TextNode.valueOf("abc"),
                true,
                startsWith(literal(VARCHAR, utf8Slice("XYZ")), variable("string_parameter"))))
                .isEqualTo(FALSE);

        // multiple inputs - returning true if any match is found
        assertThat(predicateResult(
                new ArrayNode(JsonNodeFactory.instance, ImmutableList.of(TextNode.valueOf("aBC"), TextNode.valueOf("abc"), TextNode.valueOf("Abc"))),
                TextNode.valueOf("abc"),
                true,
                startsWith(wildcardArrayAccessor(contextVariable()), literal(createVarcharType(1), utf8Slice("A")))))
                .isEqualTo(TRUE);

        // multiple inputs - returning true if any match is found. array is automatically unwrapped in lax mode
        assertThat(predicateResult(
                new ArrayNode(JsonNodeFactory.instance, ImmutableList.of(TextNode.valueOf("aBC"), TextNode.valueOf("abc"), TextNode.valueOf("Abc"))),
                TextNode.valueOf("abc"),
                true,
                startsWith(contextVariable(), literal(createVarcharType(1), utf8Slice("A")))))
                .isEqualTo(TRUE);

        // lax mode: true is returned on the first match, even if there is an uncomparable item
        assertThat(predicateResult(
                new ArrayNode(JsonNodeFactory.instance, ImmutableList.of(TextNode.valueOf("Abc"), NullNode.instance)),
                TextNode.valueOf("abc"),
                true,
                startsWith(contextVariable(), literal(createVarcharType(1), utf8Slice("A")))))
                .isEqualTo(TRUE);

        // lax mode: unknown is returned because there is an uncomparable item, even if match is found first
        assertThat(predicateResult(
                new ArrayNode(JsonNodeFactory.instance, ImmutableList.of(TextNode.valueOf("Abc"), NullNode.instance)),
                TextNode.valueOf("abc"),
                false,
                startsWith(contextVariable(), literal(createVarcharType(1), utf8Slice("A")))))
                .isEqualTo(null);

        // lax mode: unknown is returned because the uncomparable item is before the matching item
        assertThat(predicateResult(
                new ArrayNode(JsonNodeFactory.instance, ImmutableList.of(NullNode.instance, TextNode.valueOf("Abc"))),
                TextNode.valueOf("abc"),
                true,
                startsWith(contextVariable(), literal(createVarcharType(1), utf8Slice("A")))))
                .isEqualTo(null);

        // error while evaluating the first operand (floor method called on a text value) -> result unknown
        assertThat(predicateResult(
                new ArrayNode(JsonNodeFactory.instance, ImmutableList.of(NullNode.instance, TextNode.valueOf("Abc"))),
                TextNode.valueOf("abc"),
                true,
                startsWith(floor(literal(VARCHAR, utf8Slice("x"))), literal(VARCHAR, utf8Slice("A")))))
                .isEqualTo(null);

        // error while evaluating the second operand (floor method called on a text value) -> result unknown
        assertThat(predicateResult(
                new ArrayNode(JsonNodeFactory.instance, ImmutableList.of(NullNode.instance, TextNode.valueOf("Abc"))),
                TextNode.valueOf("abc"),
                true,
                startsWith(literal(VARCHAR, utf8Slice("x")), floor(literal(VARCHAR, utf8Slice("A"))))))
                .isEqualTo(null);

        // the second operand returns multiple items -> result unknown
        assertThat(predicateResult(
                new ArrayNode(JsonNodeFactory.instance, ImmutableList.of(TextNode.valueOf("A"), TextNode.valueOf("B"))),
                TextNode.valueOf("abc"),
                true,
                startsWith(literal(VARCHAR, utf8Slice("x")), wildcardArrayAccessor(contextVariable()))))
                .isEqualTo(null);

        // the second operand is not text -> result unknown
        assertThat(predicateResult(
                new ArrayNode(JsonNodeFactory.instance, ImmutableList.of(NullNode.instance, TextNode.valueOf("Abc"))),
                TextNode.valueOf("abc"),
                true,
                startsWith(literal(VARCHAR, utf8Slice("x")), literal(BIGINT, 1L))))
                .isEqualTo(null);

        // the first operand returns empty sequence -> result false
        assertThat(predicateResult(
                new ArrayNode(JsonNodeFactory.instance, ImmutableList.of(NullNode.instance, TextNode.valueOf("Abc"))),
                TextNode.valueOf("abc"),
                true,
                startsWith(arrayAccessor(contextVariable(), at(literal(BIGINT, 100L))), literal(VARCHAR, utf8Slice("A")))))
                .isEqualTo(FALSE);
    }

    @Test
    public void testFilter()
    {
        assertThat(pathResult(
                IntNode.valueOf(-5),
                path(true, filter(literal(BIGINT, 5L), greaterThan(currentItem(), literal(BIGINT, 3L)))))) // true
                .isEqualTo(singletonSequence(new TypedValue(BIGINT, 5L)));

        assertThat(pathResult(
                IntNode.valueOf(-5),
                path(true, filter(literal(BIGINT, 5L), lessThan(currentItem(), literal(BIGINT, 3L)))))) // false
                .isEqualTo(emptySequence());

        assertThat(pathResult(
                IntNode.valueOf(-5),
                path(true, filter(literal(BIGINT, 5L), lessThan(currentItem(), literal(BOOLEAN, true)))))) // unknown
                .isEqualTo(emptySequence());

        // multiple input items
        assertThat(pathResult(
                new ArrayNode(JsonNodeFactory.instance, ImmutableList.of(DoubleNode.valueOf(1.5e0), IntNode.valueOf(2), LongNode.valueOf(5), ShortNode.valueOf((short) 10))),
                path(true, filter(wildcardArrayAccessor(contextVariable()), greaterThan(currentItem(), literal(BIGINT, 3L))))))
                .isEqualTo(sequence(LongNode.valueOf(5), ShortNode.valueOf((short) 10)));

        // multiple input items -- array is automatically unwrapped in lax mode
        assertThat(pathResult(
                new ArrayNode(JsonNodeFactory.instance, ImmutableList.of(DoubleNode.valueOf(1.5e0), IntNode.valueOf(2), LongNode.valueOf(5), ShortNode.valueOf((short) 10))),
                path(true, filter(contextVariable(), greaterThan(currentItem(), literal(BIGINT, 3L))))))
                .isEqualTo(sequence(LongNode.valueOf(5), ShortNode.valueOf((short) 10)));
    }

    @Test
    public void testCurrentItemVariable()
    {
        assertThatThrownBy(() -> evaluate(
                IntNode.valueOf(-5),
                path(true, currentItem())))
                .isInstanceOf(PathEvaluationError.class)
                .hasMessage("path evaluation failed: accessing current filter item with no enclosing filter");
    }

    private static AssertProvider<? extends RecursiveComparisonAssert<?>> pathResult(JsonNode input, IrJsonPath path)
    {
        return () -> new RecursiveComparisonAssert<>(evaluate(input, path), COMPARISON_CONFIGURATION);
    }

    private static List<Object> evaluate(JsonNode input, IrJsonPath path)
    {
        Session session = testSessionBuilder().build();
        JsonPathEvaluator evaluator = new JsonPathEvaluator(
                input,
                PARAMETERS,
                createTestFunctionAndTypeManager(),
                session.getSqlFunctionProperties());
        return evaluator.evaluate(path);
    }

    private static AssertProvider<? extends RecursiveComparisonAssert<?>> predicateResult(JsonNode input, Object currentItem, boolean lax, IrPredicate predicate)
    {
        return () -> new RecursiveComparisonAssert<>(evaluatePredicate(input, currentItem, lax, predicate), COMPARISON_CONFIGURATION);
    }

    private static Boolean evaluatePredicate(JsonNode input, Object currentItem, boolean lax, IrPredicate predicate)
    {
        Session session = testSessionBuilder().build();
        JsonPredicateEvaluator evaluator = new JsonPredicateEvaluator(new JsonPathEvaluator(
                input,
                PARAMETERS,
                createTestFunctionAndTypeManager(),
                session.getSqlFunctionProperties()));
        return evaluator.evaluate(predicate, lax, new JsonPathEvaluator.Context().withCurrentItem(currentItem));
    }
}
