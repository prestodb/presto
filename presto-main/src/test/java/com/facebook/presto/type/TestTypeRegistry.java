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

import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.CharType.createCharType;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DecimalType.createDecimalType;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.facebook.presto.type.JoniRegexpType.JONI_REGEXP;
import static com.facebook.presto.type.JsonPathType.JSON_PATH;
import static com.facebook.presto.type.LikePatternType.LIKE_PATTERN;
import static com.facebook.presto.type.Re2JRegexpType.RE2J_REGEXP;
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

        assertFalse(isTypeOnlyCoercion("char(42)", "varchar(42)"));

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
        assertTrue(typeRegistry.canCoerce(VARCHAR, JONI_REGEXP));
        assertTrue(typeRegistry.canCoerce(VARCHAR, RE2J_REGEXP));
        assertTrue(typeRegistry.canCoerce(VARCHAR, LIKE_PATTERN));
        assertTrue(typeRegistry.canCoerce(VARCHAR, JSON_PATH));

        assertTrue(typeRegistry.canCoerce(REAL, DOUBLE));
        assertTrue(typeRegistry.canCoerce(TINYINT, REAL));
        assertTrue(typeRegistry.canCoerce(SMALLINT, REAL));
        assertTrue(typeRegistry.canCoerce(INTEGER, REAL));
        assertTrue(typeRegistry.canCoerce(BIGINT, REAL));

        assertFalse(typeRegistry.canCoerce(DOUBLE, BIGINT));
        assertFalse(typeRegistry.canCoerce(TIMESTAMP, TIME_WITH_TIME_ZONE));
        assertFalse(typeRegistry.canCoerce(TIMESTAMP_WITH_TIME_ZONE, TIMESTAMP));
        assertFalse(typeRegistry.canCoerce(VARBINARY, VARCHAR));
        assertFalse(canCoerce("real", "decimal(37,1)"));
        assertFalse(canCoerce("real", "decimal(37,37)"));
        assertFalse(canCoerce("double", "decimal(37,1)"));
        assertFalse(canCoerce("double", "decimal(37,37)"));

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

        assertTrue(canCoerce("char(42)", "varchar(42)"));
        assertTrue(canCoerce("char(42)", "varchar(44)"));
        assertFalse(canCoerce("char(42)", "char(44)"));
        assertFalse(canCoerce("char(42)", "char(40)"));
        assertFalse(canCoerce("char(42)", "varchar(40)"));

        assertTrue(typeRegistry.canCoerce(createType("char(42)"), JONI_REGEXP));
        assertTrue(typeRegistry.canCoerce(createType("char(42)"), RE2J_REGEXP));
        assertTrue(typeRegistry.canCoerce(createType("char(42)"), LIKE_PATTERN));
        assertTrue(typeRegistry.canCoerce(createType("char(42)"), JSON_PATH));

        assertTrue(canCoerce("decimal(22,1)", "decimal(23,1)"));
        assertFalse(canCoerce("decimal(23,1)", "decimal(22,1)"));
        assertFalse(canCoerce("bigint", "decimal(18,0)"));
        assertTrue(canCoerce("bigint", "decimal(19,0)"));
        assertTrue(canCoerce("bigint", "decimal(37,1)"));
        assertTrue(canCoerce("array(bigint)", "array(decimal(20,1))"));
        assertFalse(canCoerce("array(bigint)", "array(decimal(2,1))"));

        assertTrue(canCoerce("decimal(3,2)", "double"));
        assertTrue(canCoerce("decimal(22,1)", "double"));
        assertTrue(canCoerce("decimal(3,2)", "real"));
        assertTrue(canCoerce("decimal(22,1)", "real"));

        assertFalse(canCoerce("integer", "decimal(9,0)"));
        assertTrue(canCoerce("integer", "decimal(10,0)"));
        assertTrue(canCoerce("integer", "decimal(37,1)"));

        assertFalse(canCoerce("tinyint", "decimal(2,0)"));
        assertTrue(canCoerce("tinyint", "decimal(3,0)"));
        assertTrue(canCoerce("tinyint", "decimal(37,1)"));

        assertFalse(canCoerce("smallint", "decimal(4,0)"));
        assertTrue(canCoerce("smallint", "decimal(5,0)"));
        assertTrue(canCoerce("smallint", "decimal(37,1)"));
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
        assertCommonSuperType(VARCHAR, JONI_REGEXP, JONI_REGEXP);
        assertCommonSuperType(VARCHAR, RE2J_REGEXP, RE2J_REGEXP);
        assertCommonSuperType(VARCHAR, LIKE_PATTERN, LIKE_PATTERN);
        assertCommonSuperType(VARCHAR, JSON_PATH, JSON_PATH);

        assertCommonSuperType(REAL, DOUBLE, DOUBLE);
        assertCommonSuperType(REAL, TINYINT, REAL);
        assertCommonSuperType(REAL, SMALLINT, REAL);
        assertCommonSuperType(REAL, INTEGER, REAL);
        assertCommonSuperType(REAL, BIGINT, REAL);

        assertCommonSuperType(TIMESTAMP, TIME_WITH_TIME_ZONE, null);
        assertCommonSuperType(VARBINARY, VARCHAR, null);

        assertCommonSuperType("unknown", "array(bigint)", "array(bigint)");
        assertCommonSuperType("array(bigint)", "array(double)", "array(double)");
        assertCommonSuperType("array(bigint)", "array(unknown)", "array(bigint)");
        assertCommonSuperType("map(bigint,double)", "map(bigint,double)", "map(bigint,double)");
        assertCommonSuperType("map(bigint,double)", "map(double,double)", "map(double,double)");
        assertCommonSuperType("row(a bigint,b double,c varchar)", "row(a bigint,b double,c varchar)", "row(a bigint,b double,c varchar)");

        assertCommonSuperType("decimal(22,1)", "decimal(23,1)", "decimal(23,1)");
        assertCommonSuperType("bigint", "decimal(23,1)", "decimal(23,1)");
        assertCommonSuperType("bigint", "decimal(18,0)", "decimal(19,0)");
        assertCommonSuperType("bigint", "decimal(19,0)", "decimal(19,0)");
        assertCommonSuperType("bigint", "decimal(37,1)", "decimal(37,1)");
        assertCommonSuperType("real", "decimal(37,1)", "real");
        assertCommonSuperType("array(decimal(23,1))", "array(decimal(22,1))", "array(decimal(23,1))");
        assertCommonSuperType("array(bigint)", "array(decimal(2,1))", "array(decimal(20,1))");

        assertCommonSuperType("decimal(3,2)", "double", "double");
        assertCommonSuperType("decimal(22,1)", "double", "double");

        assertCommonSuperType("integer", "decimal(23,1)", "decimal(23,1)");
        assertCommonSuperType("integer", "decimal(9,0)", "decimal(10,0)");
        assertCommonSuperType("integer", "decimal(10,0)", "decimal(10,0)");
        assertCommonSuperType("integer", "decimal(37,1)", "decimal(37,1)");

        assertCommonSuperType("tinyint", "decimal(2,0)", "decimal(3,0)");
        assertCommonSuperType("tinyint", "decimal(9,0)", "decimal(9,0)");
        assertCommonSuperType("tinyint", "decimal(2,1)", "decimal(4,1)");

        assertCommonSuperType("smallint", "decimal(2,0)", "decimal(5,0)");
        assertCommonSuperType("smallint", "decimal(9,0)", "decimal(9,0)");
        assertCommonSuperType("smallint", "decimal(2,1)", "decimal(6,1)");
    }

    @Test
    public void testCoerceTypeBase()
            throws Exception
    {
        assertEquals(typeRegistry.coerceTypeBase(createDecimalType(21, 1), "decimal"), Optional.of(createDecimalType(21, 1)));
        assertEquals(typeRegistry.coerceTypeBase(BIGINT, "decimal"), Optional.of(createDecimalType(19, 0)));
        assertEquals(typeRegistry.coerceTypeBase(INTEGER, "decimal"), Optional.of(createDecimalType(10, 0)));
        assertEquals(typeRegistry.coerceTypeBase(TINYINT, "decimal"), Optional.of(createDecimalType(3, 0)));
        assertEquals(typeRegistry.coerceTypeBase(SMALLINT, "decimal"), Optional.of(createDecimalType(5, 0)));
    }

    @Test
    public void testCanCoerceIsTransitive()
            throws Exception
    {
        Set<Type> types = getStandardPrimitiveTypes();
        for (Type transitiveType : types) {
            for (Type resultType : types) {
                if (typeRegistry.canCoerce(transitiveType, resultType)) {
                    for (Type sourceType : types) {
                        if (typeRegistry.canCoerce(sourceType, transitiveType)) {
                            if (!typeRegistry.canCoerce(sourceType, resultType)) {
                                fail(format("'%s' -> '%s' coercion is missing when transitive coercion is possible: '%s' -> '%s' -> '%s'",
                                        sourceType, resultType, sourceType, transitiveType, resultType));
                            }
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testCastOperatorsExistForCoercions()
    {
        FunctionRegistry functionRegistry = new FunctionRegistry(typeRegistry, new BlockEncodingManager(typeRegistry), new FeaturesConfig().setExperimentalSyntaxEnabled(true));

        Set<Type> types = getStandardPrimitiveTypes();
        for (Type sourceType : types) {
            for (Type resultType : types) {
                if (typeRegistry.canCoerce(sourceType, resultType) && sourceType != UNKNOWN && resultType != UNKNOWN) {
                    assertTrue(functionRegistry.canResolveOperator(OperatorType.CAST, resultType, ImmutableList.of(sourceType)),
                            format("'%s' -> '%s' coercion exists but there is no cast operator", sourceType, resultType));
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
        builder.add(createCharType(0));
        builder.add(createCharType(42));
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
