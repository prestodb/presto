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
package com.facebook.presto.hive;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.Subfield;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.ValueSet;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.DomainTranslator.ExtractionResult;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.MapType;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDomainTranslator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.spi.function.OperatorType.SUBSCRIPT;
import static com.facebook.presto.spi.predicate.TupleDomain.withColumnDomains;
import static com.facebook.presto.spi.relation.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.DEREFERENCE;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IN;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.specialForm;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.testng.Assert.assertEquals;

public class TestDomainTranslator
{
    private static final VariableReferenceExpression C_BIGINT = new VariableReferenceExpression("c_bigint", BIGINT);
    private static final VariableReferenceExpression C_BIGINT_ARRAY = new VariableReferenceExpression("c_bigint_array", new ArrayType(BIGINT));
    private static final VariableReferenceExpression C_BIGINT_TO_BIGINT_MAP = new VariableReferenceExpression("c_bigint_to_bigint_map", mapType(BIGINT, BIGINT));
    private static final VariableReferenceExpression C_VARCHAR_TO_BIGINT_MAP = new VariableReferenceExpression("c_varchar_to_bigint_map", mapType(VARCHAR, BIGINT));
    private static final VariableReferenceExpression C_STRUCT = new VariableReferenceExpression("c_struct", RowType.from(ImmutableList.of(
            RowType.field("a", BIGINT),
            RowType.field("b", RowType.from(ImmutableList.of(RowType.field("x", BIGINT)))),
            RowType.field("c", new ArrayType(BIGINT)),
            RowType.field("d", mapType(BIGINT, BIGINT)),
            RowType.field("e", mapType(VARCHAR, BIGINT)))));

    private Metadata metadata;
    private RowExpressionDomainTranslator domainTranslator;
    private SubfieldExtractor columnExtractor;

    @BeforeClass
    public void setup()
    {
        metadata = createTestMetadataManager();
        domainTranslator = new RowExpressionDomainTranslator(metadata);
        columnExtractor = new SubfieldExtractor(new FunctionResolution(metadata.getFunctionManager()));
    }

    @Test
    public void testSubfields()
    {
        Map<String, RowExpression> expressions = ImmutableMap.<String, RowExpression>builder()
                .put("c_bigint", C_BIGINT)
                .put("c_bigint_array[5]", arraySubscript(C_BIGINT_ARRAY, 5))
                .put("c_bigint_to_bigint_map[5]", mapSubscript(C_BIGINT_TO_BIGINT_MAP, constant(5, BIGINT)))
                .put("c_varchar_to_bigint_map[\"foo\"]", mapSubscript(C_VARCHAR_TO_BIGINT_MAP, constant("foo", VARCHAR)))
                .put("c_struct.a", dereference(C_STRUCT, 0))
                .put("c_struct.b.x", dereference(dereference(C_STRUCT, 1), 0))
                .put("c_struct.c[5]", arraySubscript(dereference(C_STRUCT, 2), 5))
                .put("c_struct.d[5]", mapSubscript(dereference(C_STRUCT, 3), constant(5, BIGINT)))
                .put("c_struct.e[\"foo\"]", mapSubscript(dereference(C_STRUCT, 4), constant("foo", VARCHAR)))
                .build();

        for (Map.Entry<String, RowExpression> entry : expressions.entrySet()) {
            String subfield = entry.getKey();
            RowExpression expression = entry.getValue();

            assertPredicateTranslates(greaterThan(expression, bigintLiteral(2L)), subfield, Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 2L)), false));

            assertPredicateTranslates(equal(expression, bigintLiteral(2L)), subfield, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 2L)), false));

            assertPredicateTranslates(between(expression, bigintLiteral(1L), bigintLiteral(2L)), subfield, Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 1L, true, 2L, true)), false));

            assertPredicateTranslates(bigintIn(expression, ImmutableList.of(1L)), subfield, Domain.singleValue(BIGINT, 1L));

            assertPredicateTranslates(bigintIn(expression, ImmutableList.of(1L, 2L)), subfield, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L)), false));
        }
    }

    private RowExpression dereference(RowExpression base, int field)
    {
        Type fieldType = base.getType().getTypeParameters().get(field);
        return specialForm(DEREFERENCE, fieldType, ImmutableList.of(base, new ConstantExpression(field, INTEGER)));
    }

    private RowExpression arraySubscript(RowExpression arrayExpression, int index)
    {
        ArrayType arrayType = (ArrayType) arrayExpression.getType();
        Type elementType = arrayType.getElementType();
        return call(SUBSCRIPT.name(),
                operator(SUBSCRIPT, arrayType, elementType),
                elementType,
                ImmutableList.of(arrayExpression, constant(index, INTEGER)));
    }

    private RowExpression mapSubscript(RowExpression mapExpression, RowExpression keyExpression)
    {
        MapType mapType = (MapType) mapExpression.getType();
        return call(SUBSCRIPT.name(),
                operator(SUBSCRIPT, mapType(mapType.getKeyType(), mapType.getValueType()), mapType.getKeyType()),
                mapType.getValueType(),
                ImmutableList.of(mapExpression, keyExpression));
    }

    private static MapType mapType(Type keyType, Type valueType)
    {
        return new MapType(
                keyType,
                valueType,
                methodHandle(TestDomainTranslator.class, "throwUnsupportedOperationException"),
                methodHandle(TestDomainTranslator.class, "throwUnsupportedOperationException"),
                methodHandle(TestDomainTranslator.class, "throwUnsupportedOperationException"),
                methodHandle(TestDomainTranslator.class, "throwUnsupportedOperationException"));
    }

    public static void throwUnsupportedOperationException()
    {
        throw new UnsupportedOperationException();
    }

    private FunctionHandle operator(OperatorType operatorType, Type... types)
    {
        return metadata.getFunctionManager().resolveOperator(operatorType, fromTypes(types));
    }

    private void assertPredicateTranslates(RowExpression predicate, String subfield, Domain domain)
    {
        ExtractionResult<Subfield> result = domainTranslator.fromPredicate(TEST_SESSION.toConnectorSession(), predicate, columnExtractor);
        assertEquals(result.getRemainingExpression(), TRUE_CONSTANT);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(new Subfield(subfield), domain)));
    }

    private RowExpression greaterThan(RowExpression left, RowExpression right)
    {
        return binaryOperator(OperatorType.GREATER_THAN, left, right);
    }

    private RowExpression equal(RowExpression left, RowExpression right)
    {
        return binaryOperator(OperatorType.EQUAL, left, right);
    }

    private RowExpression between(RowExpression value, RowExpression min, RowExpression max)
    {
        return call(
                OperatorType.BETWEEN.name(),
                metadata.getFunctionManager().resolveOperator(OperatorType.BETWEEN, fromTypes(value.getType(), min.getType(), max.getType())),
                BOOLEAN,
                value,
                min,
                max);
    }

    private RowExpression bigintIn(RowExpression value, List<Long> inList)
    {
        List<RowExpression> arguments = inList.stream().map(argument -> constant(argument, BIGINT)).collect(toImmutableList());
        return in(value, arguments);
    }

    private RowExpression in(RowExpression value, List<RowExpression> inList)
    {
        return new SpecialFormExpression(IN, BOOLEAN, ImmutableList.<RowExpression>builder().add(value).addAll(inList).build());
    }

    private RowExpression binaryOperator(OperatorType operatorType, RowExpression left, RowExpression right)
    {
        return call(
                operatorType.name(),
                metadata.getFunctionManager().resolveOperator(operatorType, fromTypes(left.getType(), right.getType())),
                BOOLEAN,
                left,
                right);
    }

    private RowExpression bigintLiteral(long value)
    {
        return constant(value, BIGINT);
    }
}
