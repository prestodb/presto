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
package com.facebook.presto.spi.predicate;

import com.facebook.presto.spi.type.TestingTypeDeserializer;
import com.facebook.presto.spi.type.TestingTypeManager;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.slice.Slices;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.HyperLogLogType.HYPER_LOG_LOG;

public class TestAllOrNoneValueSet
{
    @Test
    public void testAll()
            throws Exception
    {
        AllOrNoneValueSet valueSet = AllOrNoneValueSet.all(HYPER_LOG_LOG);
        Assert.assertEquals(valueSet.getType(), HYPER_LOG_LOG);
        Assert.assertFalse(valueSet.isNone());
        Assert.assertTrue(valueSet.isAll());
        Assert.assertFalse(valueSet.isSingleValue());
        Assert.assertTrue(valueSet.containsValue(Slices.EMPTY_SLICE));

        try {
            valueSet.getSingleValue();
            Assert.fail();
        }
        catch (Exception ignored) {
        }
    }

    @Test
    public void testNone()
            throws Exception
    {
        AllOrNoneValueSet valueSet = AllOrNoneValueSet.none(HYPER_LOG_LOG);
        Assert.assertEquals(valueSet.getType(), HYPER_LOG_LOG);
        Assert.assertTrue(valueSet.isNone());
        Assert.assertFalse(valueSet.isAll());
        Assert.assertFalse(valueSet.isSingleValue());
        Assert.assertFalse(valueSet.containsValue(Slices.EMPTY_SLICE));

        try {
            valueSet.getSingleValue();
            Assert.fail();
        }
        catch (Exception ignored) {
        }
    }

    @Test
    public void testIntersect()
            throws Exception
    {
        AllOrNoneValueSet all = AllOrNoneValueSet.all(HYPER_LOG_LOG);
        AllOrNoneValueSet none = AllOrNoneValueSet.none(HYPER_LOG_LOG);

        Assert.assertEquals(all.intersect(all), all);
        Assert.assertEquals(all.intersect(none), none);
        Assert.assertEquals(none.intersect(all), none);
        Assert.assertEquals(none.intersect(none), none);
    }

    @Test
    public void testUnion()
            throws Exception
    {
        AllOrNoneValueSet all = AllOrNoneValueSet.all(HYPER_LOG_LOG);
        AllOrNoneValueSet none = AllOrNoneValueSet.none(HYPER_LOG_LOG);

        Assert.assertEquals(all.union(all), all);
        Assert.assertEquals(all.union(none), all);
        Assert.assertEquals(none.union(all), all);
        Assert.assertEquals(none.union(none), none);
    }

    @Test
    public void testComplement()
            throws Exception
    {
        AllOrNoneValueSet all = AllOrNoneValueSet.all(HYPER_LOG_LOG);
        AllOrNoneValueSet none = AllOrNoneValueSet.none(HYPER_LOG_LOG);

        Assert.assertEquals(all.complement(), none);
        Assert.assertEquals(none.complement(), all);
    }

    @Test
    public void testOverlaps()
            throws Exception
    {
        AllOrNoneValueSet all = AllOrNoneValueSet.all(HYPER_LOG_LOG);
        AllOrNoneValueSet none = AllOrNoneValueSet.none(HYPER_LOG_LOG);

        Assert.assertTrue(all.overlaps(all));
        Assert.assertFalse(all.overlaps(none));
        Assert.assertFalse(none.overlaps(all));
        Assert.assertFalse(none.overlaps(none));
    }

    @Test
    public void testSubtract()
            throws Exception
    {
        AllOrNoneValueSet all = AllOrNoneValueSet.all(HYPER_LOG_LOG);
        AllOrNoneValueSet none = AllOrNoneValueSet.none(HYPER_LOG_LOG);

        Assert.assertEquals(all.subtract(all), none);
        Assert.assertEquals(all.subtract(none), all);
        Assert.assertEquals(none.subtract(all), none);
        Assert.assertEquals(none.subtract(none), none);
    }

    @Test
    public void testContains()
            throws Exception
    {
        AllOrNoneValueSet all = AllOrNoneValueSet.all(HYPER_LOG_LOG);
        AllOrNoneValueSet none = AllOrNoneValueSet.none(HYPER_LOG_LOG);

        Assert.assertTrue(all.contains(all));
        Assert.assertTrue(all.contains(none));
        Assert.assertFalse(none.contains(all));
        Assert.assertTrue(none.contains(none));
    }

    @Test
    public void testJsonSerialization()
            throws Exception
    {
        TestingTypeManager typeManager = new TestingTypeManager();

        ObjectMapper mapper = new ObjectMapperProvider().get()
                .registerModule(new SimpleModule().addDeserializer(Type.class, new TestingTypeDeserializer(typeManager)));

        AllOrNoneValueSet all = AllOrNoneValueSet.all(HYPER_LOG_LOG);
        Assert.assertEquals(all, mapper.readValue(mapper.writeValueAsString(all), AllOrNoneValueSet.class));

        AllOrNoneValueSet none = AllOrNoneValueSet.none(HYPER_LOG_LOG);
        Assert.assertEquals(none, mapper.readValue(mapper.writeValueAsString(none), AllOrNoneValueSet.class));
    }
}
