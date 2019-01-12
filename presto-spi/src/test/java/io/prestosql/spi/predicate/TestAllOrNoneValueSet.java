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
package io.prestosql.spi.predicate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.slice.Slices;
import io.prestosql.spi.type.TestingTypeDeserializer;
import io.prestosql.spi.type.TestingTypeManager;
import io.prestosql.spi.type.Type;
import org.testng.annotations.Test;

import static io.prestosql.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestAllOrNoneValueSet
{
    @Test
    public void testAll()
    {
        AllOrNoneValueSet valueSet = AllOrNoneValueSet.all(HYPER_LOG_LOG);
        assertEquals(valueSet.getType(), HYPER_LOG_LOG);
        assertFalse(valueSet.isNone());
        assertTrue(valueSet.isAll());
        assertFalse(valueSet.isSingleValue());
        assertTrue(valueSet.containsValue(Slices.EMPTY_SLICE));

        try {
            valueSet.getSingleValue();
            fail();
        }
        catch (Exception ignored) {
        }
    }

    @Test
    public void testNone()
    {
        AllOrNoneValueSet valueSet = AllOrNoneValueSet.none(HYPER_LOG_LOG);
        assertEquals(valueSet.getType(), HYPER_LOG_LOG);
        assertTrue(valueSet.isNone());
        assertFalse(valueSet.isAll());
        assertFalse(valueSet.isSingleValue());
        assertFalse(valueSet.containsValue(Slices.EMPTY_SLICE));

        try {
            valueSet.getSingleValue();
            fail();
        }
        catch (Exception ignored) {
        }
    }

    @Test
    public void testIntersect()
    {
        AllOrNoneValueSet all = AllOrNoneValueSet.all(HYPER_LOG_LOG);
        AllOrNoneValueSet none = AllOrNoneValueSet.none(HYPER_LOG_LOG);

        assertEquals(all.intersect(all), all);
        assertEquals(all.intersect(none), none);
        assertEquals(none.intersect(all), none);
        assertEquals(none.intersect(none), none);
    }

    @Test
    public void testUnion()
    {
        AllOrNoneValueSet all = AllOrNoneValueSet.all(HYPER_LOG_LOG);
        AllOrNoneValueSet none = AllOrNoneValueSet.none(HYPER_LOG_LOG);

        assertEquals(all.union(all), all);
        assertEquals(all.union(none), all);
        assertEquals(none.union(all), all);
        assertEquals(none.union(none), none);
    }

    @Test
    public void testComplement()
    {
        AllOrNoneValueSet all = AllOrNoneValueSet.all(HYPER_LOG_LOG);
        AllOrNoneValueSet none = AllOrNoneValueSet.none(HYPER_LOG_LOG);

        assertEquals(all.complement(), none);
        assertEquals(none.complement(), all);
    }

    @Test
    public void testOverlaps()
    {
        AllOrNoneValueSet all = AllOrNoneValueSet.all(HYPER_LOG_LOG);
        AllOrNoneValueSet none = AllOrNoneValueSet.none(HYPER_LOG_LOG);

        assertTrue(all.overlaps(all));
        assertFalse(all.overlaps(none));
        assertFalse(none.overlaps(all));
        assertFalse(none.overlaps(none));
    }

    @Test
    public void testSubtract()
    {
        AllOrNoneValueSet all = AllOrNoneValueSet.all(HYPER_LOG_LOG);
        AllOrNoneValueSet none = AllOrNoneValueSet.none(HYPER_LOG_LOG);

        assertEquals(all.subtract(all), none);
        assertEquals(all.subtract(none), all);
        assertEquals(none.subtract(all), none);
        assertEquals(none.subtract(none), none);
    }

    @Test
    public void testContains()
    {
        AllOrNoneValueSet all = AllOrNoneValueSet.all(HYPER_LOG_LOG);
        AllOrNoneValueSet none = AllOrNoneValueSet.none(HYPER_LOG_LOG);

        assertTrue(all.contains(all));
        assertTrue(all.contains(none));
        assertFalse(none.contains(all));
        assertTrue(none.contains(none));
    }

    @Test
    public void testJsonSerialization()
            throws Exception
    {
        TestingTypeManager typeManager = new TestingTypeManager();

        ObjectMapper mapper = new ObjectMapperProvider().get()
                .registerModule(new SimpleModule().addDeserializer(Type.class, new TestingTypeDeserializer(typeManager)));

        AllOrNoneValueSet all = AllOrNoneValueSet.all(HYPER_LOG_LOG);
        assertEquals(all, mapper.readValue(mapper.writeValueAsString(all), AllOrNoneValueSet.class));

        AllOrNoneValueSet none = AllOrNoneValueSet.none(HYPER_LOG_LOG);
        assertEquals(none, mapper.readValue(mapper.writeValueAsString(none), AllOrNoneValueSet.class));
    }
}
