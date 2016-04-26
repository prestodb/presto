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

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DecimalType.createDecimalType;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.facebook.presto.type.JsonPathType.JSON_PATH;
import static com.facebook.presto.type.LikePatternType.LIKE_PATTERN;
import static com.facebook.presto.type.RegexpType.REGEXP;
import static com.facebook.presto.type.UnknownType.UNKNOWN;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestTypeRegistry
{
    private final TypeRegistry typeRegistry = new TypeRegistry();

    @Test
    public void testIsTypeOnlyCoercion()
    {
        assertTrue(typeRegistry.isTypeOnlyCoercion(BIGINT, BIGINT));
        assertTrue(isTypeOnlyCoercion("varchar(42)", "varchar(44)"));
        assertFalse(isTypeOnlyCoercion("varchar(44)", "varchar(42)"));

        assertTrue(isTypeOnlyCoercion("array(varchar(42))", "array(varchar(44))"));
        assertFalse(isTypeOnlyCoercion("array(varchar(44))", "array(varchar(42))"));

        assertTrue(isTypeOnlyCoercion("decimal(22,1)", "decimal(23,1)"));
        assertTrue(isTypeOnlyCoercion("decimal(2,1)", "decimal(3,1)"));
        assertFalse(isTypeOnlyCoercion("decimal(23,1)", "decimal(22,1)"));
        assertFalse(isTypeOnlyCoercion("decimal(3,1)", "decimal(2,1)"));
        assertFalse(isTypeOnlyCoercion("decimal(3,1)", "decimal(22,1)"));

        assertTrue(isTypeOnlyCoercion("array(decimal(22,1))", "array(decimal(23,1))"));
        assertTrue(isTypeOnlyCoercion("array(decimal(2,1))", "array(decimal(3,1))"));
        assertFalse(isTypeOnlyCoercion("array(decimal(23,1))", "array(decimal(22,1))"));
        assertFalse(isTypeOnlyCoercion("array(decimal(3,1))", "array(decimal(2,1))"));

        assertTrue(isTypeOnlyCoercion("map(decimal(2,1), decimal(2,1))", "map(decimal(2,1), decimal(3,1))"));
        assertFalse(isTypeOnlyCoercion("map(decimal(2,1), decimal(2,1))", "map(decimal(2,1), decimal(23,1))"));
        assertFalse(isTypeOnlyCoercion("map(decimal(2,1), decimal(2,1))", "map(decimal(2,1), decimal(3,2))"));
        assertTrue(isTypeOnlyCoercion("map(decimal(22,1), decimal(2,1))", "map(decimal(23,1), decimal(3,1))"));
        assertFalse(isTypeOnlyCoercion("map(decimal(23,1), decimal(3,1))", "map(decimal(22,1), decimal(2,1))"));
    }

    @Test
    public void testCanCoerce()
    {
        assertTrue(typeRegistry.canCoerce(BIGINT, BIGINT));
        assertTrue(typeRegistry.canCoerce(UNKNOWN, BIGINT));
        assertFalse(typeRegistry.canCoerce(BIGINT, UNKNOWN));

        assertTrue(typeRegistry.canCoerce(BIGINT, DOUBLE));
        assertTrue(typeRegistry.canCoerce(DATE, TIMESTAMP));
        assertTrue(typeRegistry.canCoerce(DATE, TIMESTAMP_WITH_TIME_ZONE));
        assertTrue(typeRegistry.canCoerce(TIME, TIME_WITH_TIME_ZONE));
        assertTrue(typeRegistry.canCoerce(TIMESTAMP, TIMESTAMP_WITH_TIME_ZONE));
        assertTrue(typeRegistry.canCoerce(VARCHAR, REGEXP));
        assertTrue(typeRegistry.canCoerce(VARCHAR, LIKE_PATTERN));
        assertTrue(typeRegistry.canCoerce(VARCHAR, JSON_PATH));

        assertFalse(typeRegistry.canCoerce(DOUBLE, BIGINT));
        assertFalse(typeRegistry.canCoerce(TIMESTAMP, TIME_WITH_TIME_ZONE));
        assertFalse(typeRegistry.canCoerce(TIMESTAMP_WITH_TIME_ZONE, TIMESTAMP));
        assertFalse(typeRegistry.canCoerce(VARBINARY, VARCHAR));

        assertTrue(canCoerce("unknown", "array(bigint)"));
        assertFalse(canCoerce("array(bigint)", "unknown"));
        assertTrue(canCoerce("array(bigint)", "array(double)"));
        assertFalse(canCoerce("array(double)", "array(bigint)"));
        assertTrue(canCoerce("map(bigint,double)", "map(bigint,double)"));
        assertTrue(canCoerce("map(bigint,double)", "map(double,double)"));
        assertTrue(canCoerce("row(a bigint,b double,c varchar)", "row(a bigint,b double,c varchar)"));

        assertTrue(canCoerce("varchar(42)", "varchar(42)"));
        assertTrue(canCoerce("varchar(42)", "varchar(44)"));
        assertFalse(canCoerce("varchar(44)", "varchar(42)"));
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
        assertCommonSuperType("row(a bigint,b double,c varchar)", "row(a bigint,b double,c varchar)", "row(a bigint,b double,c varchar)");

        assertCommonSuperType("varchar(42)", "varchar(44)", "varchar(44)");
    }

    @Test
    public void testCanCoerceIsTransitive()
            throws Exception
    {
        Set<Type> types = getStandardPrimitiveTypes();
        for (Type sourceType : types) {
            for (Type resultType : types) {
                if (typeRegistry.canCoerce(sourceType, resultType)) {
                    for (Type transitiveType : types) {
                        if (typeRegistry.canCoerce(transitiveType, sourceType) && !typeRegistry.canCoerce(transitiveType, resultType)) {
                            fail(format("'%s' -> '%s' coercion is missing when transitive coercion is possible: '%s' -> '%s' -> '%s'",
                                    transitiveType, resultType, transitiveType, sourceType, resultType));
                        }
                    }
                }
            }
        }
    }

    private Set<Type> getStandardPrimitiveTypes()
    {
        ImmutableSet.Builder<Type> builder = ImmutableSet.builder();
        // add unparametrized types
        builder.addAll(typeRegistry.getTypes());
        // add corner cases for parametrized types
        builder.add(createDecimalType(1, 0));
        builder.add(createDecimalType(17, 0));
        builder.add(createDecimalType(38, 0));
        builder.add(createDecimalType(17, 17));
        builder.add(createDecimalType(38, 38));
        builder.add(createVarcharType(0));
        builder.add(createUnboundedVarcharType());
        return builder.build();
    }

    private void assertCommonSuperType(Type firstType, Type secondType, Type expected)
    {
        TypeRegistry typeManager = new TypeRegistry();
        assertEquals(typeManager.getCommonSuperType(firstType, secondType), Optional.ofNullable(expected));
        assertEquals(typeManager.getCommonSuperType(secondType, firstType), Optional.ofNullable(expected));
    }

    private void assertCommonSuperType(String firstType, String secondType, String expected)
    {
        assertEquals(typeRegistry.getCommonSuperType(createType(firstType), createType(secondType)), Optional.ofNullable(expected).map(this::createType));
        assertEquals(typeRegistry.getCommonSuperType(createType(secondType), createType(firstType)), Optional.ofNullable(expected).map(this::createType));
    }

    private boolean canCoerce(String actual, String expected)
    {
        return typeRegistry.canCoerce(createType(actual), createType(expected));
    }

    private boolean isTypeOnlyCoercion(String actual, String expected)
    {
        return typeRegistry.isTypeOnlyCoercion(createType(actual), createType(expected));
    }

    private Type createType(String signature)
    {
        return typeRegistry.getType(TypeSignature.parseTypeSignature(signature));
    }
}
