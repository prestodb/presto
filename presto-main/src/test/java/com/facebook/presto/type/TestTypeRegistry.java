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

import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.type.JsonPathType.JSON_PATH;
import static com.facebook.presto.type.LikePatternType.LIKE_PATTERN;
import static com.facebook.presto.type.RegexpType.REGEXP;
import static com.facebook.presto.type.TypeRegistry.getCommonSuperTypeSignature;
import static com.facebook.presto.type.UnknownType.UNKNOWN;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestTypeRegistry
{
    private final TypeSignature varcharX = new TypeSignature(StandardTypes.VARCHAR, ImmutableList.of(TypeSignatureParameter.of("x")));
    private final TypeSignature decimalPS = new TypeSignature(StandardTypes.DECIMAL,
            ImmutableList.of(TypeSignatureParameter.of("p"), TypeSignatureParameter.of("n")));

    @Test
    public void testIsTypeOnlyCoercion()
    {
        assertTrue(TypeRegistry.isTypeOnlyCoercion(BIGINT.getTypeSignature(), BIGINT.getTypeSignature()));
        assertTrue(TypeRegistry.isTypeOnlyCoercion(parseTypeSignature("varchar(42)"), parseTypeSignature("varchar(44)")));
        assertFalse(TypeRegistry.isTypeOnlyCoercion(parseTypeSignature("varchar(44)"), parseTypeSignature("varchar(42)")));

        assertTrue(TypeRegistry.isTypeOnlyCoercion(parseTypeSignature("array(varchar(42))"), parseTypeSignature("array(varchar(44))")));
        assertFalse(TypeRegistry.isTypeOnlyCoercion(parseTypeSignature("array(varchar(44))"), parseTypeSignature("array(varchar(42))")));

        assertTrue(TypeRegistry.isTypeOnlyCoercion(parseTypeSignature("decimal(22,1)"), parseTypeSignature("decimal(23,1)")));
        assertTrue(TypeRegistry.isTypeOnlyCoercion(parseTypeSignature("decimal(2,1)"), parseTypeSignature("decimal(3,1)")));
        assertFalse(TypeRegistry.isTypeOnlyCoercion(parseTypeSignature("decimal(23,1)"), parseTypeSignature("decimal(22,1)")));
        assertFalse(TypeRegistry.isTypeOnlyCoercion(parseTypeSignature("decimal(3,1)"), parseTypeSignature("decimal(2,1)")));

        assertTrue(TypeRegistry.isTypeOnlyCoercion(parseTypeSignature("array(decimal(22,1))"), parseTypeSignature("array(decimal(23,1))")));
        assertTrue(TypeRegistry.isTypeOnlyCoercion(parseTypeSignature("array(decimal(2,1))"), parseTypeSignature("array(decimal(3,1))")));
        assertFalse(TypeRegistry.isTypeOnlyCoercion(parseTypeSignature("array(decimal(23,1))"), parseTypeSignature("array(decimal(22,1))")));
        assertFalse(TypeRegistry.isTypeOnlyCoercion(parseTypeSignature("array(decimal(3,1))"), parseTypeSignature("array(decimal(2,1))")));

        assertTrue(TypeRegistry.isTypeOnlyCoercion(parseTypeSignature("map(decimal(2,1), decimal(2,1))"), parseTypeSignature("map(decimal(2,1), decimal(3,1))")));
        assertFalse(TypeRegistry.isTypeOnlyCoercion(parseTypeSignature("map(decimal(2,1), decimal(2,1))"), parseTypeSignature("map(decimal(2,1), decimal(23,1))")));
        assertFalse(TypeRegistry.isTypeOnlyCoercion(parseTypeSignature("map(decimal(2,1), decimal(2,1))"), parseTypeSignature("map(decimal(2,1), decimal(3,2))")));
        assertTrue(TypeRegistry.isTypeOnlyCoercion(parseTypeSignature("map(decimal(22,1), decimal(2,1))"), parseTypeSignature("map(decimal(23,1), decimal(3,1))")));
        assertFalse(TypeRegistry.isTypeOnlyCoercion(parseTypeSignature("map(decimal(23,1), decimal(3,1))"), parseTypeSignature("map(decimal(22,1), decimal(2,1))")));
    }

    @Test
    public void testCanCoerce()
    {
        assertTrue(TypeRegistry.canCoerce(BIGINT, BIGINT));
        assertTrue(TypeRegistry.canCoerce(UNKNOWN, BIGINT));
        assertFalse(TypeRegistry.canCoerce(BIGINT, UNKNOWN));

        assertTrue(TypeRegistry.canCoerce(BIGINT, DOUBLE));
        assertTrue(TypeRegistry.canCoerce(DATE, TIMESTAMP));
        assertTrue(TypeRegistry.canCoerce(DATE, TIMESTAMP_WITH_TIME_ZONE));
        assertTrue(TypeRegistry.canCoerce(TIME, TIME_WITH_TIME_ZONE));
        assertTrue(TypeRegistry.canCoerce(TIMESTAMP, TIMESTAMP_WITH_TIME_ZONE));
        assertTrue(TypeRegistry.canCoerce(VARCHAR, REGEXP));
        assertTrue(TypeRegistry.canCoerce(VARCHAR, LIKE_PATTERN));
        assertTrue(TypeRegistry.canCoerce(VARCHAR, JSON_PATH));

        assertFalse(TypeRegistry.canCoerce(DOUBLE, BIGINT));
        assertFalse(TypeRegistry.canCoerce(TIMESTAMP, TIME_WITH_TIME_ZONE));
        assertFalse(TypeRegistry.canCoerce(TIMESTAMP_WITH_TIME_ZONE, TIMESTAMP));
        assertFalse(TypeRegistry.canCoerce(VARBINARY, VARCHAR));

        assertTrue(TypeRegistry.canCoerce(UNKNOWN.getTypeSignature(), parseTypeSignature("array(bigint)")));
        assertFalse(TypeRegistry.canCoerce(parseTypeSignature("array(bigint)"), UNKNOWN.getTypeSignature()));
        assertTrue(TypeRegistry.canCoerce(parseTypeSignature("array(bigint)"), parseTypeSignature("array(double)")));
        assertFalse(TypeRegistry.canCoerce(parseTypeSignature("array(double)"), parseTypeSignature("array(bigint)")));
        assertTrue(TypeRegistry.canCoerce(parseTypeSignature("map(bigint,double)"), parseTypeSignature("map(bigint,double)")));
        assertTrue(TypeRegistry.canCoerce(parseTypeSignature("map(bigint,double)"), parseTypeSignature("map(double,double)")));
        assertTrue(TypeRegistry.canCoerce(parseTypeSignature("row<bigint,double,varchar>('a','b','c')"), parseTypeSignature("row<bigint,double,varchar>('a','b','c')")));

        assertTrue(TypeRegistry.canCoerce(parseTypeSignature("varchar(42)"), parseTypeSignature("varchar(42)")));
        assertTrue(TypeRegistry.canCoerce(parseTypeSignature("varchar(42)"), parseTypeSignature("varchar(44)")));
        assertFalse(TypeRegistry.canCoerce(parseTypeSignature("varchar(44)"), parseTypeSignature("varchar(42)")));
        assertTrue(TypeRegistry.canCoerce(parseTypeSignature("varchar(44)"), parseTypeSignature("varchar")));
        assertTrue(TypeRegistry.canCoerce(parseTypeSignature("varchar(44)"), varcharX));
        assertTrue(TypeRegistry.canCoerce(varcharX, parseTypeSignature("varchar(44)")));
        assertTrue(TypeRegistry.canCoerce(UNKNOWN.getTypeSignature(), parseTypeSignature("varchar")));
        assertTrue(TypeRegistry.canCoerce(UNKNOWN.getTypeSignature(), parseTypeSignature("varchar(44)")));
        assertTrue(TypeRegistry.canCoerce(UNKNOWN.getTypeSignature(), varcharX));

        assertTrue(TypeRegistry.canCoerce(parseTypeSignature("decimal(22,1)"), parseTypeSignature("decimal(23,1)")));
        assertTrue(TypeRegistry.canCoerce(parseTypeSignature("decimal"), parseTypeSignature("decimal(38,1)")));
        assertTrue(TypeRegistry.canCoerce(parseTypeSignature("decimal(22,1)"), decimalPS));
        assertTrue(TypeRegistry.canCoerce(decimalPS, parseTypeSignature("decimal(23,1)")));
        assertFalse(TypeRegistry.canCoerce(parseTypeSignature("decimal(23,1)"), parseTypeSignature("decimal(22,1)")));

        assertFalse(TypeRegistry.canCoerce(BIGINT.getTypeSignature(), parseTypeSignature("decimal(18,0)")));
        assertTrue(TypeRegistry.canCoerce(BIGINT.getTypeSignature(), parseTypeSignature("decimal(19,0)")));
        assertTrue(TypeRegistry.canCoerce(BIGINT.getTypeSignature(), parseTypeSignature("decimal(37,1)")));
        assertTrue(TypeRegistry.canCoerce(BIGINT.getTypeSignature(), parseTypeSignature("decimal")));
        assertTrue(TypeRegistry.canCoerce(BIGINT.getTypeSignature(), decimalPS));

        assertTrue(TypeRegistry.canCoerce(parseTypeSignature("array(decimal(2,1))"), parseTypeSignature("array(decimal)")));
        assertTrue(TypeRegistry.canCoerce(parseTypeSignature("array(bigint)"), parseTypeSignature("array(decimal)")));
        assertFalse(TypeRegistry.canCoerce(parseTypeSignature("array(bigint)"), parseTypeSignature("array(decimal(2,1))")));

        assertTrue(TypeRegistry.canCoerce(parseTypeSignature("decimal(22,1)"), DOUBLE.getTypeSignature()));
        assertTrue(TypeRegistry.canCoerce(parseTypeSignature("decimal"), DOUBLE.getTypeSignature()));
        assertTrue(TypeRegistry.canCoerce(decimalPS, DOUBLE.getTypeSignature()));

        assertFalse(TypeRegistry.canCoerce(INTEGER.getTypeSignature(), parseTypeSignature("decimal(9,0)")));
        assertTrue(TypeRegistry.canCoerce(INTEGER.getTypeSignature(), parseTypeSignature("decimal(10,0)")));
        assertTrue(TypeRegistry.canCoerce(INTEGER.getTypeSignature(), parseTypeSignature("decimal(37,1)")));
        assertTrue(TypeRegistry.canCoerce(INTEGER.getTypeSignature(), parseTypeSignature("decimal")));
        assertTrue(TypeRegistry.canCoerce(INTEGER.getTypeSignature(), decimalPS));
    }

    @Test
    public void testGetCommonSuperType()
    {
        assertCommonSuperType(UNKNOWN, UNKNOWN, UNKNOWN);
        assertCommonSuperType(BIGINT, BIGINT, BIGINT);
        assertCommonSuperType(UNKNOWN, BIGINT, BIGINT);

        assertCommonSuperType(BIGINT, DOUBLE, DOUBLE);
        assertCommonSuperType(DATE, TIMESTAMP, TIMESTAMP);
        assertCommonSuperType(DATE, TIMESTAMP_WITH_TIME_ZONE, TIMESTAMP_WITH_TIME_ZONE);
        assertCommonSuperType(TIME, TIME_WITH_TIME_ZONE, TIME_WITH_TIME_ZONE);
        assertCommonSuperType(TIMESTAMP, TIMESTAMP_WITH_TIME_ZONE, TIMESTAMP_WITH_TIME_ZONE);
        assertCommonSuperType(VARCHAR, REGEXP, REGEXP);
        assertCommonSuperType(VARCHAR, LIKE_PATTERN, LIKE_PATTERN);
        assertCommonSuperType(VARCHAR, JSON_PATH, JSON_PATH);

        assertCommonSuperType(TIMESTAMP, TIME_WITH_TIME_ZONE, null);
        assertCommonSuperType(VARBINARY, VARCHAR, null);

        assertCommonSuperType("unknown", "array(bigint)", "array(bigint)");
        assertCommonSuperType("array(bigint)", "array(double)", "array(double)");
        assertCommonSuperType("array(bigint)", "array(unknown)", "array(bigint)");
        assertCommonSuperType("map(bigint,double)", "map(bigint,double)", "map(bigint,double)");
        assertCommonSuperType("map(bigint,double)", "map(double,double)", "map(double,double)");
        assertCommonSuperType("row<bigint,double,varchar>('a','b','c')", "row<bigint,double,varchar>('a','b','c')", "row<bigint,double,varchar>('a','b','c')");

        assertCommonSuperType("varchar(42)", "varchar(44)", "varchar(44)");
        assertCommonSuperType("varchar(42)", "varchar", "varchar(42)");
        assertCommonSuperType(varcharX, parseTypeSignature("varchar(44)"), parseTypeSignature("varchar(44)"));

        assertCommonSuperType("decimal(22,1)", "decimal(23,1)", "decimal(23,1)");
        assertCommonSuperType("decimal", "decimal(23,1)", "decimal(23,1)");
        assertCommonSuperType(decimalPS, parseTypeSignature("decimal(23,1)"), parseTypeSignature("decimal(23,1)"));

        assertCommonSuperType(BIGINT.getTypeSignature(), parseTypeSignature("decimal(23,1)"), parseTypeSignature("decimal(23,1)"));
        assertCommonSuperType(BIGINT.getTypeSignature(), parseTypeSignature("decimal(18,0)"), parseTypeSignature("decimal(19,0)"));
        assertCommonSuperType(BIGINT.getTypeSignature(), parseTypeSignature("decimal(19,0)"), parseTypeSignature("decimal(19,0)"));
        assertCommonSuperType(BIGINT.getTypeSignature(), parseTypeSignature("decimal(37,1)"), parseTypeSignature("decimal(37,1)"));
        assertCommonSuperType(BIGINT.getTypeSignature(), parseTypeSignature("decimal"), parseTypeSignature("decimal(19,0)"));
        assertCommonSuperType(BIGINT.getTypeSignature(), decimalPS, parseTypeSignature("decimal(19,0)"));

        assertCommonSuperType("array(decimal(23,1))", "array(decimal(22,1))", "array(decimal(23,1))");
        assertCommonSuperType("array(bigint)", "array(decimal(2,1))", "array(decimal(20,1))");

        assertCommonSuperType("decimal(22,1)", "double", "double");
        assertCommonSuperType("decimal", "double", "double");
        assertCommonSuperType(decimalPS, DOUBLE.getTypeSignature(), DOUBLE.getTypeSignature());

        assertCommonSuperType(INTEGER.getTypeSignature(), parseTypeSignature("decimal(23,1)"), parseTypeSignature("decimal(23,1)"));
        assertCommonSuperType(INTEGER.getTypeSignature(), parseTypeSignature("decimal(9,0)"), parseTypeSignature("decimal(10,0)"));
        assertCommonSuperType(INTEGER.getTypeSignature(), parseTypeSignature("decimal(10,0)"), parseTypeSignature("decimal(10,0)"));
        assertCommonSuperType(INTEGER.getTypeSignature(), parseTypeSignature("decimal(37,1)"), parseTypeSignature("decimal(37,1)"));
        assertCommonSuperType(INTEGER.getTypeSignature(), parseTypeSignature("decimal"), parseTypeSignature("decimal(10,0)"));
        assertCommonSuperType(INTEGER.getTypeSignature(), decimalPS, parseTypeSignature("decimal(10,0)"));
    }

    private void assertCommonSuperType(Type firstType, Type secondType, Type expected)
    {
        TypeRegistry typeManager = new TypeRegistry();
        assertEquals(typeManager.getCommonSuperType(firstType, secondType), Optional.ofNullable(expected));
        assertEquals(typeManager.getCommonSuperType(secondType, firstType), Optional.ofNullable(expected));
    }

    private void assertCommonSuperType(String firstType, String secondType, String expected)
    {
        TypeSignature expectedType = expected == null ? null : parseTypeSignature(expected);
        assertEquals(getCommonSuperTypeSignature(parseTypeSignature(firstType), parseTypeSignature(secondType)), Optional.ofNullable(expectedType));
        assertEquals(getCommonSuperTypeSignature(parseTypeSignature(secondType), parseTypeSignature(firstType)), Optional.ofNullable(expectedType));
    }

    private void assertCommonSuperType(TypeSignature firstType, TypeSignature secondType, TypeSignature expected)
    {
        assertEquals(getCommonSuperTypeSignature(firstType, secondType), Optional.ofNullable(expected));
        assertEquals(getCommonSuperTypeSignature(secondType, firstType), Optional.ofNullable(expected));
    }
}
