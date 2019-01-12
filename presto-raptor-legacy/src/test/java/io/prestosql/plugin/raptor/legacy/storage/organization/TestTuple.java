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
package io.prestosql.plugin.raptor.legacy.storage.organization;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.type.Type;
import org.testng.annotations.Test;

import java.util.List;

import static io.airlift.testing.Assertions.assertGreaterThan;
import static io.airlift.testing.Assertions.assertLessThan;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static org.testng.Assert.assertEquals;

public class TestTuple
{
    @Test
    public void testComparableTuple()
    {
        List<Type> types = ImmutableList.of(BIGINT, VARCHAR, BOOLEAN, DOUBLE, DATE, TIMESTAMP);

        Tuple tuple1 = new Tuple(types, ImmutableList.of(1L, "hello", false, 1.2d, 11111, 1112));
        Tuple equalToTuple1 = new Tuple(types, ImmutableList.of(1L, "hello", false, 1.2d, 11111, 1112));
        Tuple greaterThanTuple1 = new Tuple(types, ImmutableList.of(1L, "hello", false, 1.2d, 11111, 1113));
        Tuple lessThanTuple1 = new Tuple(types, ImmutableList.of(1L, "hello", false, 1.2d, 11111, 1111));

        assertEquals(tuple1.compareTo(equalToTuple1), 0);
        assertLessThan(tuple1.compareTo(greaterThanTuple1), 0);
        assertGreaterThan(tuple1.compareTo(lessThanTuple1), 0);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "types must be the same")
    public void testMismatchedTypes()
    {
        List<Type> types1 = ImmutableList.of(createVarcharType(3));
        List<Type> types2 = ImmutableList.of(createVarcharType(4));
        Tuple tuple1 = new Tuple(types1, ImmutableList.of("abc"));
        Tuple tuple2 = new Tuple(types2, ImmutableList.of("abcd"));
        tuple1.compareTo(tuple2);
    }
}
