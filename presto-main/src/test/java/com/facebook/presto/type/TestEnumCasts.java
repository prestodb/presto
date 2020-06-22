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
import com.facebook.presto.common.type.IntegerEnumType;
import com.facebook.presto.common.type.StringEnumType;
import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.sql.analyzer.SemanticErrorCode;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertThrows;

public class TestEnumCasts
        extends AbstractTestFunctions
{
    private static final Long BIG_VALUE = Integer.MAX_VALUE + 10L; // 2147483657

    private static final IntegerEnumType MOOD_ENUM = new IntegerEnumType("Mood", ImmutableMap.of(
            "HAPPY", 0L,
            "SAD", 1L,
            "MELLOW", BIG_VALUE));
    private static final StringEnumType COUNTRY_ENUM = new StringEnumType("Country", ImmutableMap.of(
            "US", "United States",
            "BAHAMAS", "The Bahamas"));

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
    public void testInvalidEnumWithDuplicates()
    {
        assertThrows(NotSupportedException.class,
                () -> new StringEnumType("my_invalid_enum",
                        ImmutableMap.of(
                                "k1", "value",
                                "k2", "value")));
    }
}
