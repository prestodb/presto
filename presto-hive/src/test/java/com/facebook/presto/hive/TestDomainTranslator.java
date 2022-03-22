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

import com.facebook.presto.cache.CacheConfig;
import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.ValueSet;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.DomainTranslator;
import com.facebook.presto.spi.relation.DomainTranslator.ExtractionResult;
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDomainTranslator;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.block.BlockAssertions.createArrayBigintBlock;
import static com.facebook.presto.block.BlockAssertions.createMapBlock;
import static com.facebook.presto.common.function.OperatorType.SUBSCRIPT;
import static com.facebook.presto.common.predicate.TupleDomain.withColumnDomains;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.hive.HiveTestUtils.mapType;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.spi.relation.ConstantExpression.createConstantExpression;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.DEREFERENCE;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IN;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IS_NULL;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.specialForm;
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

    private static final ExpressionOptimizer TEST_EXPRESSION_OPTIMIZER = new ExpressionOptimizer()
    {
        @Override
        public RowExpression optimize(RowExpression rowExpression, Level level, ConnectorSession session)
        {
            return rowExpression;
        }

        @Override
        public Object optimize(RowExpression expression, Level level, ConnectorSession session, Function<VariableReferenceExpression, Object> variableResolver)
        {
            throw new UnsupportedOperationException();
        }
    };

    private Metadata metadata;
    private RowExpressionDomainTranslator domainTranslator;
    private DomainTranslator.ColumnExtractor<Subfield> columnExtractor;

    @BeforeClass
    public void setup()
    {
        metadata = createTestMetadataManager();
        domainTranslator = new RowExpressionDomainTranslator(metadata);
        columnExtractor = new SubfieldExtractor(
                new FunctionResolution(metadata.getFunctionAndTypeManager()),
                TEST_EXPRESSION_OPTIMIZER,
                new TestingConnectorSession(
                        new HiveSessionProperties(
                                new HiveClientConfig().setRangeFiltersOnSubscriptsEnabled(true),
                                new OrcFileWriterConfig(),
                                new ParquetFileWriterConfig(),
                                new CacheConfig()).getSessionProperties())).toColumnExtractor();
    }

    @Test
    public void testSubfields()
    {
        Map<String, RowExpression> expressions = ImmutableMap.<String, RowExpression>builder()
                .put("c_bigint", C_BIGINT)
                .put("c_bigint_array[5]", arraySubscript(C_BIGINT_ARRAY, 5))
                .put("c_bigint_to_bigint_map[5]", mapSubscript(C_BIGINT_TO_BIGINT_MAP, constant(5L, BIGINT)))
                .put("c_varchar_to_bigint_map[\"foo\"]", mapSubscript(C_VARCHAR_TO_BIGINT_MAP, constant(Slices.utf8Slice("foo"), VARCHAR)))
                .put("c_struct.a", dereference(C_STRUCT, 0))
                .put("c_struct.b.x", dereference(dereference(C_STRUCT, 1), 0))
                .put("c_struct.c[5]", arraySubscript(dereference(C_STRUCT, 2), 5))
                .put("c_struct.d[5]", mapSubscript(dereference(C_STRUCT, 3), constant(5L, BIGINT)))
                .put("c_struct.e[\"foo\"]", mapSubscript(dereference(C_STRUCT, 4), constant(Slices.utf8Slice("foo"), VARCHAR)))
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

        Type arrayType = C_BIGINT_ARRAY.getType();
        assertPredicateTranslates(isNull(C_BIGINT_ARRAY), C_BIGINT_ARRAY.getName(), Domain.create(ValueSet.none(arrayType), true));
        assertPredicateTranslates(not(isNull(C_BIGINT_ARRAY)), C_BIGINT_ARRAY.getName(), Domain.create(ValueSet.all(arrayType), false));
        assertPredicateDoesNotTranslate(equal(C_BIGINT_ARRAY, createConstantExpression(createArrayBigintBlock(ImmutableList.of(ImmutableList.of(1L, 2L, 3L))), arrayType)));

        MapType mapType = (MapType) C_BIGINT_TO_BIGINT_MAP.getType();
        assertPredicateTranslates(isNull(C_BIGINT_TO_BIGINT_MAP), C_BIGINT_TO_BIGINT_MAP.getName(), Domain.create(ValueSet.none(mapType), true));
        assertPredicateTranslates(not(isNull(C_BIGINT_TO_BIGINT_MAP)), C_BIGINT_TO_BIGINT_MAP.getName(), Domain.create(ValueSet.all(mapType), false));
        assertPredicateDoesNotTranslate(equal(C_BIGINT_TO_BIGINT_MAP, createConstantExpression(createMapBlock(mapType, ImmutableMap.of(1, 100)), mapType)));
    }

    private RowExpression dereference(RowExpression base, int field)
    {
        Type fieldType = base.getType().getTypeParameters().get(field);
        return specialForm(DEREFERENCE, fieldType, ImmutableList.of(base, new ConstantExpression((long) field, INTEGER)));
    }

    private RowExpression isNull(RowExpression expression)
    {
        return specialForm(IS_NULL, BOOLEAN, expression);
    }

    private RowExpression not(RowExpression expression)
    {
        return call("not", new FunctionResolution(metadata.getFunctionAndTypeManager()).notFunction(), BOOLEAN, expression);
    }

    private RowExpression arraySubscript(RowExpression arrayExpression, int index)
    {
        ArrayType arrayType = (ArrayType) arrayExpression.getType();
        Type elementType = arrayType.getElementType();
        return call(SUBSCRIPT.name(),
                operator(SUBSCRIPT, arrayType, elementType),
                elementType,
                ImmutableList.of(arrayExpression, constant((long) index, INTEGER)));
    }

    private RowExpression mapSubscript(RowExpression mapExpression, RowExpression keyExpression)
    {
        MapType mapType = (MapType) mapExpression.getType();
        return call(SUBSCRIPT.name(),
                operator(SUBSCRIPT, mapType(mapType.getKeyType(), mapType.getValueType()), mapType.getKeyType()),
                mapType.getValueType(),
                ImmutableList.of(mapExpression, keyExpression));
    }

    private FunctionHandle operator(OperatorType operatorType, Type... types)
    {
        return metadata.getFunctionAndTypeManager().resolveOperator(operatorType, fromTypes(types));
    }

    private void assertPredicateTranslates(RowExpression predicate, String subfield, Domain domain)
    {
        ExtractionResult<Subfield> result = domainTranslator.fromPredicate(TEST_SESSION.toConnectorSession(), predicate, columnExtractor);
        assertEquals(result.getRemainingExpression(), TRUE_CONSTANT);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(new Subfield(subfield), domain)));
    }

    private void assertPredicateDoesNotTranslate(RowExpression predicate)
    {
        ExtractionResult<Subfield> result = domainTranslator.fromPredicate(TEST_SESSION.toConnectorSession(), predicate, columnExtractor);
        assertEquals(result.getTupleDomain(), TupleDomain.all());
        assertEquals(result.getRemainingExpression(), predicate);
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
                metadata.getFunctionAndTypeManager().resolveOperator(OperatorType.BETWEEN, fromTypes(value.getType(), min.getType(), max.getType())),
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
                metadata.getFunctionAndTypeManager().resolveOperator(operatorType, fromTypes(left.getType(), right.getType())),
                BOOLEAN,
                left,
                right);
    }

    private RowExpression bigintLiteral(long value)
    {
        return constant(value, BIGINT);
    }
}
