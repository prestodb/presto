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
package io.prestosql.operator.scalar;

import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.Type;
import org.testng.annotations.Test;

import java.util.List;
import java.util.stream.IntStream;

import static io.prestosql.operator.scalar.ZipFunction.MAX_ARITY;
import static io.prestosql.operator.scalar.ZipFunction.MIN_ARITY;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static io.prestosql.type.UnknownType.UNKNOWN;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;

public class TestZipFunction
        extends AbstractTestFunctions
{
    @Test
    public void testSameLength()
    {
        assertFunction("zip(ARRAY[1, 2], ARRAY['a', 'b'])",
                zipReturnType(INTEGER, createVarcharType(1)),
                list(list(1, "a"), list(2, "b")));

        assertFunction("zip(ARRAY[1, 2], ARRAY['a', CAST('b' AS VARCHAR)])",
                zipReturnType(INTEGER, VARCHAR),
                list(list(1, "a"), list(2, "b")));

        assertFunction("zip(ARRAY[1, 2, 3, 4], ARRAY['a', 'b', 'c', 'd'])",
                zipReturnType(INTEGER, createVarcharType(1)),
                list(list(1, "a"), list(2, "b"), list(3, "c"), list(4, "d")));

        assertFunction("zip(ARRAY[1, 2], ARRAY['a', 'b'],  ARRAY['c', 'd'])",
                zipReturnType(INTEGER, createVarcharType(1), createVarcharType(1)),
                list(list(1, "a", "c"), list(2, "b", "d")));

        assertFunction("zip(ARRAY[1, 2], ARRAY['a', 'b'],  ARRAY['c', 'd'], ARRAY['e', 'f'])",
                zipReturnType(INTEGER, createVarcharType(1), createVarcharType(1), createVarcharType(1)),
                list(list(1, "a", "c", "e"), list(2, "b", "d", "f")));

        assertFunction("zip(ARRAY[], ARRAY[])",
                zipReturnType(UNKNOWN, UNKNOWN),
                list());

        assertFunction("zip(ARRAY[], ARRAY[], ARRAY[])",
                zipReturnType(UNKNOWN, UNKNOWN, UNKNOWN),
                list());

        assertFunction("zip(ARRAY[], ARRAY[], ARRAY[], ARRAY[])",
                zipReturnType(UNKNOWN, UNKNOWN, UNKNOWN, UNKNOWN),
                list());

        assertFunction("zip(ARRAY[NULL], ARRAY[NULL])",
                zipReturnType(UNKNOWN, UNKNOWN),
                list(list(null, null)));

        assertFunction("zip(ARRAY[ARRAY[1, 1], ARRAY[1, 2]], ARRAY[ARRAY[2, 1], ARRAY[2, 2]])",
                zipReturnType(new ArrayType(INTEGER), new ArrayType(INTEGER)),
                list(list(list(1, 1), list(2, 1)), list(list(1, 2), list(2, 2))));
    }

    @Test
    public void testDifferentLength()
    {
        assertFunction("zip(ARRAY[1], ARRAY['a', 'b'])",
                zipReturnType(INTEGER, createVarcharType(1)),
                list(list(1, "a"), list(null, "b")));

        assertFunction("zip(ARRAY[NULL, 2], ARRAY['a'])",
                zipReturnType(INTEGER, createVarcharType(1)),
                list(list(null, "a"), list(2, null)));

        assertFunction("zip(ARRAY[], ARRAY[1], ARRAY[1, 2], ARRAY[1, 2, 3])",
                zipReturnType(UNKNOWN, INTEGER, INTEGER, INTEGER),
                list(list(null, 1, 1, 1), list(null, null, 2, 2), list(null, null, null, 3)));

        assertFunction("zip(ARRAY[], ARRAY[NULL], ARRAY[NULL, NULL])",
                zipReturnType(UNKNOWN, UNKNOWN, UNKNOWN),
                list(list(null, null, null), list(null, null, null)));
    }

    @Test
    public void testWithNull()
    {
        assertFunction("zip(CAST(NULL AS ARRAY(UNKNOWN)), ARRAY[],  ARRAY[1])",
                zipReturnType(UNKNOWN, UNKNOWN, INTEGER),
                null);
    }

    @Test
    public void testAllArities()
    {
        for (int arity = MIN_ARITY; arity <= MAX_ARITY; arity++) {
            String[] arguments = IntStream.rangeClosed(1, arity)
                    .mapToObj(index -> "ARRAY[" + index + "]")
                    .toArray(String[]::new);
            Type[] types = IntStream.rangeClosed(1, arity)
                    .mapToObj(index -> INTEGER)
                    .toArray(Type[]::new);
            assertFunction(
                    format("zip(%s)", join(", ", list(arguments))),
                    zipReturnType(types),
                    list(IntStream.rangeClosed(1, arity).boxed().collect(toList())));
        }
    }

    private static Type zipReturnType(Type... types)
    {
        return new ArrayType(RowType.anonymous(list(types)));
    }

    @SafeVarargs
    private static <T> List<T> list(T... a)
    {
        return asList(a);
    }
}
