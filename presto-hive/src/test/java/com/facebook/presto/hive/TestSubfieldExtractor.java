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

import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.spi.Subfield;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.MapType;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.spi.function.OperatorType.SUBSCRIPT;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.DEREFERENCE;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.specialForm;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.facebook.presto.util.Reflection.methodHandle;

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

    private FunctionManager functionManager;
    private SubfieldExtractor subfieldExtractor;

    @BeforeClass
    public void setup()
    {
        functionManager = createTestMetadataManager().getFunctionManager();
        subfieldExtractor = new SubfieldExtractor(new FunctionResolution(functionManager));
    }

    @Test
    public void test()
    {
        assertSubfieldExtract(C_BIGINT, "c_bigint");
        assertSubfieldExtract(arraySubscript(C_BIGINT_ARRAY, 5), "c_bigint_array[5]");
        assertSubfieldExtract(mapSubscript(C_BIGINT_TO_BIGINT_MAP, constant(5, BIGINT)), "c_bigint_to_bigint_map[5]");
        assertSubfieldExtract(mapSubscript(C_VARCHAR_TO_BIGINT_MAP, constant("foo", VARCHAR)), "c_varchar_to_bigint_map[\"foo\"]");
        assertSubfieldExtract(dereference(C_STRUCT, 0), "c_struct.a");
        assertSubfieldExtract(dereference(dereference(C_STRUCT, 1), 0), "c_struct.b.x");
        assertSubfieldExtract(arraySubscript(dereference(C_STRUCT, 2), 5), "c_struct.c[5]");
        assertSubfieldExtract(mapSubscript(dereference(C_STRUCT, 3), constant(5, BIGINT)), "c_struct.d[5]");
        assertSubfieldExtract(mapSubscript(dereference(C_STRUCT, 4), constant("foo", VARCHAR)), "c_struct.e[\"foo\"]");

        assertEquals(subfieldExtractor.extract(constant(2, INTEGER)), Optional.empty());
    }

    private void assertSubfieldExtract(RowExpression expression, String subfield)
    {
        assertEquals(subfieldExtractor.extract(expression), Optional.of(new Subfield(subfield)));
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
        return functionManager.resolveOperator(operatorType, fromTypes(types));
    }
}
