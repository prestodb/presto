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

import com.facebook.presto.common.NotSupportedException;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.IntegerEnumType;
import com.facebook.presto.common.type.StringEnumType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.sql.analyzer.SemanticErrorCode;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertThrows;

public class TestEnumOperators
        extends AbstractTestFunctions
{
    private static final Long BIG_VALUE = Integer.MAX_VALUE + 10L; // 2147483657

    private static final IntegerEnumType MOOD_ENUM = new IntegerEnumType("Mood", ImmutableMap.of(
            "HAPPY", 0L,
            "SAD", 1L,
            "MELLOW", BIG_VALUE));
    private static final StringEnumType COUNTRY_ENUM = new StringEnumType("Country", ImmutableMap.of(
            "US", "United States",
            "BAHAMAS", "The Bahamas",
            "FRANCE", "France"));

    @BeforeClass
    public void initEnumTypes()
    {
        functionAssertions.getTypeRegistry().addType(MOOD_ENUM);
        functionAssertions.getTypeRegistry().addType(COUNTRY_ENUM);
    }

    private void assertUnavailableCast(String projection)
    {
        assertInvalidFunction(projection, SemanticErrorCode.TYPE_MISMATCH);
    }

    @Test
    public void testEnumLiterals()
    {
        assertFunction("Mood 'HAPPY'", MOOD_ENUM, 0L);
        assertFunction("Mood 'MELLOW'", MOOD_ENUM, BIG_VALUE);
        assertFunction("Country 'US'", COUNTRY_ENUM, "United States");
        assertFunction("Country 'The Bahamas'", COUNTRY_ENUM, "The Bahamas");
    }

    @Test
    public void testInvalidEnumLiterals()
    {
        assertInvalidCast("Mood 'happy'");
        assertInvalidCast("Mood '0'");
        assertInvalidCast("Country 'not_valid'");
    }

    @Test
    public void testCastToEnum()
    {
        assertFunction("CAST(2147483657 AS Mood)", MOOD_ENUM, BIG_VALUE);
        assertFunction("CAST(CAST(1 AS TINYINT) AS Mood)", MOOD_ENUM, 1L);
        assertFunction("CAST(CAST(1 AS INTEGER) AS Mood)", MOOD_ENUM, 1L);
        assertFunction("CAST(CAST(1 AS SMALLINT) AS Mood)", MOOD_ENUM, 1L);
        assertFunction("CAST(CAST(1 AS BIGINT) AS Mood)", MOOD_ENUM, 1L);

        assertInvalidCast("CAST(5 AS Mood)");
        assertUnavailableCast("CAST(1.0 AS Mood)");

        assertFunction("CAST('United States' AS Country)", COUNTRY_ENUM, "United States");
        assertFunction("CAST('BAHAMAS' AS Country)", COUNTRY_ENUM, "The Bahamas");
        assertFunction("CAST('The Bahamas' AS Country)", COUNTRY_ENUM, "The Bahamas");

        assertInvalidCast("CAST('hello' AS Country)");
        assertUnavailableCast("CAST(1 AS Country)");
    }

    @Test
    public void testCastFromEnum()
    {
        assertFunction("CAST(mood'MELLOW' AS BIGINT)", BigintType.BIGINT, BIG_VALUE);
        assertFunction("CAST(country'BAHAMAS' AS VARCHAR)", VarcharType.VARCHAR, "The Bahamas");
    }

    @Test
    public void testEquality()
    {
        assertFunction("mood'HAPPY' = mood'HAPPY'", BooleanType.BOOLEAN, true);
        assertFunction("mood'HAPPY' != mood'SAD'", BooleanType.BOOLEAN, true);
        assertFunction("mood'HAPPY' = mood'SAD'", BooleanType.BOOLEAN, false);
        assertFunction("mood'HAPPY' = try_cast(NULL AS mood)", BooleanType.BOOLEAN, null);

        assertFunction("country'BAHAMAS' = country'BAHAMAS'", BooleanType.BOOLEAN, true);

        assertFunction("array[mood'HAPPY', mood'SAD'] = array[mood'HAPPY', mood'SAD']", BooleanType.BOOLEAN, true);
        assertFunction("row(mood'HAPPY', country'US') != row(mood'HAPPY', country'BAHAMAS')", BooleanType.BOOLEAN, true);

        assertInvalidFunction("mood'HAPPY' = country'US'", SemanticErrorCode.TYPE_MISMATCH);
        assertInvalidFunction("mood'HAPPY' = 3", SemanticErrorCode.TYPE_MISMATCH);
        assertInvalidFunction("row(mood'HAPPY', country'US') != row(country'US', mood'HAPPY')", SemanticErrorCode.TYPE_MISMATCH);
    }

    @Test
    public void testInListPredicate()
    {
        assertFunction("mood'HAPPY' IN (mood'SAD', mood'HAPPY', null)", BooleanType.BOOLEAN, true);
        assertFunction("mood'HAPPY' IN (mood'SAD', null)", BooleanType.BOOLEAN, null);
        assertFunction("mood'HAPPY' IN (mood'SAD', mood'MELLOW')", BooleanType.BOOLEAN, false);

        assertFunction("country'US' IN (country'BAHAMAS', country'FRANCE')", BooleanType.BOOLEAN, false);
        assertFunction("country'US' IN (country'US', country'BAHAMAS', null)", BooleanType.BOOLEAN, true);
        assertFunction("country'US' IN (country'BAHAMAS', null)", BooleanType.BOOLEAN, null);

        assertInvalidFunction("mood'HAPPY' IN ('orange', 'yellow')", SemanticErrorCode.TYPE_MISMATCH);
    }

    @Test
    public void testDistinctFrom()
    {
        assertOperator(OperatorType.IS_DISTINCT_FROM, "mood'HAPPY', mood'SAD'", BooleanType.BOOLEAN, true);
        assertOperator(OperatorType.IS_DISTINCT_FROM, "mood'HAPPY', mood'HAPPY'", BooleanType.BOOLEAN, false);
        assertOperator(OperatorType.IS_DISTINCT_FROM, "mood'HAPPY', try_cast(NULL as mood)", BooleanType.BOOLEAN, true);

        assertOperator(OperatorType.IS_DISTINCT_FROM, "country'FRANCE', country'US'", BooleanType.BOOLEAN, true);
        assertOperator(OperatorType.IS_DISTINCT_FROM, "country'FRANCE', country'FRANCE'", BooleanType.BOOLEAN, false);
        assertOperator(OperatorType.IS_DISTINCT_FROM, "country'FRANCE', try_cast(NULL as country)", BooleanType.BOOLEAN, true);
    }

    @Test
    public void testInvalidEnumWithDuplicates()
    {
        assertThrows(NotSupportedException.class,
                () -> new StringEnumType("my_invalid_enum",
                        ImmutableMap.of(
                                "k1", "value",
                                "k2", "value")));
    }
}
