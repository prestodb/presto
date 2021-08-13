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

import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.NamedTypeSignature;
import com.facebook.presto.common.type.RowFieldName;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.TestingSession;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.facebook.presto.common.function.OperatorType.SUBSCRIPT;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.hive.HiveTestUtils.mapType;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.DEREFERENCE;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.specialForm;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.testng.Assert.assertTrue;

public class TestSubfieldExtractor
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

    private FunctionAndTypeManager functionAndTypeManager;
    private SubfieldExtractor subfieldExtractor;

    @BeforeClass
    public void setup()
    {
        functionAndTypeManager = createTestMetadataManager().getFunctionAndTypeManager();
        subfieldExtractor = new SubfieldExtractor(
                new FunctionResolution(functionAndTypeManager),
                TEST_EXPRESSION_OPTIMIZER,
                TestingSession.SESSION);
    }

    @Test
    public void test()
    {
        assertSubfieldExtract(C_BIGINT, "c_bigint");
        assertSubfieldExtract(arraySubscript(C_BIGINT_ARRAY, 5), "c_bigint_array[5]");
        assertSubfieldExtract(mapSubscript(C_BIGINT_TO_BIGINT_MAP, constant(5L, BIGINT)), "c_bigint_to_bigint_map[5]");
        assertSubfieldExtract(mapSubscript(C_VARCHAR_TO_BIGINT_MAP, constant(Slices.utf8Slice("foo"), VARCHAR)), "c_varchar_to_bigint_map[\"foo\"]");
        assertSubfieldExtract(dereference(C_STRUCT, 0), "c_struct.a");
        assertSubfieldExtract(dereference(dereference(C_STRUCT, 1), 0), "c_struct.b.x");
        assertSubfieldExtract(arraySubscript(dereference(C_STRUCT, 2), 5), "c_struct.c[5]");
        assertSubfieldExtract(mapSubscript(dereference(C_STRUCT, 3), constant(5L, BIGINT)), "c_struct.d[5]");
        assertSubfieldExtract(mapSubscript(dereference(C_STRUCT, 4), constant(Slices.utf8Slice("foo"), VARCHAR)), "c_struct.e[\"foo\"]");

        assertEquals(subfieldExtractor.extract(constant(2L, INTEGER)), Optional.empty());
    }

    @Test
    public void testToRowExpression()
    {
        assertToRowExpression("a", DATE);
        assertToRowExpression("a[1]", new ArrayType(INTEGER));
        assertToRowExpression("a.b", rowType(ImmutableMap.of("b", VARCHAR, "c", DOUBLE)));
        assertToRowExpression("a[1]", mapType(BIGINT, DOUBLE));
        assertToRowExpression("a[\"hello\"]", mapType(VARCHAR, REAL));
        assertToRowExpression("a[\"hello\"].b", mapType(VARCHAR, rowType(ImmutableMap.of("b", BIGINT))));
        assertToRowExpression("a[\"hello\"].b.c", mapType(VARCHAR, rowType(ImmutableMap.of("b", rowType(ImmutableMap.of("c", BIGINT))))));
    }

    private void assertToRowExpression(String subfieldPath, Type type)
    {
        Subfield subfield = new Subfield(subfieldPath);
        Optional<Subfield> recreatedSubfield = subfieldExtractor.extract(subfieldExtractor.toRowExpression(subfield, type));
        assertTrue(recreatedSubfield.isPresent());
        assertEquals(recreatedSubfield.get(), subfield);
    }

    private static RowType rowType(Map<String, Type> fields)
    {
        return HiveTestUtils.rowType(fields.entrySet().stream()
                .map(entry -> new NamedTypeSignature(Optional.of(new RowFieldName(entry.getKey(), false)), entry.getValue().getTypeSignature()))
                .collect(toImmutableList()));
    }

    private void assertSubfieldExtract(RowExpression expression, String subfield)
    {
        assertEquals(subfieldExtractor.extract(expression), Optional.of(new Subfield(subfield)));
    }

    private RowExpression dereference(RowExpression base, int field)
    {
        Type fieldType = base.getType().getTypeParameters().get(field);
        return specialForm(DEREFERENCE, fieldType, ImmutableList.of(base, new ConstantExpression((long) field, INTEGER)));
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
        return functionAndTypeManager.resolveOperator(operatorType, fromTypes(types));
    }
}
