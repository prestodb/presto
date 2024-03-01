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
package com.facebook.presto.operator.scalar.sql;

import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.util.StructuralTestUtil.mapType;

public class TestMapNormalize
        extends AbstractTestFunctions
{
    @Test
    public void testMapNormalize()
    {
        assertFunction(
                "map_normalize(map(array['w', 'x', 'y', 'z'], array[1, 1, 1, 1]))",
                mapType(VARCHAR, DOUBLE),
                ImmutableMap.of("w", 0.25, "x", 0.25, "y", 0.25, "z", 0.25));
        assertFunction(
                "map_normalize(map(array['w', 'x', 'y', 'z'], array[1.0, 2, 3.0, 4]))",
                mapType(VARCHAR, DOUBLE),
                ImmutableMap.of("w", 0.1, "x", 0.2, "y", 0.3, "z", 0.4));
        assertFunction(
                "map_normalize(map(array['w', 'x', 'y', 'z'], array[1, 2, -3, -4]))",
                mapType(VARCHAR, DOUBLE),
                ImmutableMap.of("w", -0.25, "x", -0.5, "y", 0.75, "z", 1.0));

        Map<String, Double> expectedWithNull = new HashMap<>();
        expectedWithNull.put("w", 1.0 / 7);
        expectedWithNull.put("x", 2.0 / 7);
        expectedWithNull.put("y", null);
        expectedWithNull.put("z", 4.0 / 7);
        assertFunction("map_normalize(map(array['w', 'x', 'y', 'z'], array[1, 2, null, 4.0]))", mapType(VARCHAR, DOUBLE), expectedWithNull);

        Map<String, Double> expectedNullOnly = new HashMap<>();
        expectedNullOnly.put("w", null);
        assertFunction("map_normalize(map(array['w'], array[null]))", mapType(VARCHAR, DOUBLE), expectedNullOnly);

        assertFunction("map_normalize(map(array[], array[]))", mapType(VARCHAR, DOUBLE), ImmutableMap.of());
        assertFunction("map_normalize(null)", mapType(VARCHAR, DOUBLE), null);
    }
}
