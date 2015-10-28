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

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.TestingBlockEncodingSerde;
import com.facebook.presto.spi.block.TestingBlockJsonSerde;
import com.facebook.presto.spi.type.TestingTypeDeserializer;
import com.facebook.presto.spi.type.TestingTypeManager;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.airlift.json.ObjectMapperProvider;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Iterator;

import static com.facebook.presto.spi.type.TestingIdType.ID;

public class TestEquatableValueSet
{
    @Test
    public void testEmptySet()
            throws Exception
    {
        EquatableValueSet equatables = EquatableValueSet.none(ID);
        Assert.assertEquals(equatables.getType(), ID);
        Assert.assertTrue(equatables.isNone());
        Assert.assertFalse(equatables.isAll());
        Assert.assertFalse(equatables.isSingleValue());
        Assert.assertTrue(equatables.isWhiteList());
        Assert.assertEquals(equatables.getValues().size(), 0);
        Assert.assertEquals(equatables.complement(), EquatableValueSet.all(ID));
        Assert.assertFalse(equatables.containsValue(0L));
        Assert.assertFalse(equatables.containsValue(1L));
    }

    @Test
    public void testEntireSet()
            throws Exception
    {
        EquatableValueSet equatables = EquatableValueSet.all(ID);
        Assert.assertEquals(equatables.getType(), ID);
        Assert.assertFalse(equatables.isNone());
        Assert.assertTrue(equatables.isAll());
        Assert.assertFalse(equatables.isSingleValue());
        Assert.assertFalse(equatables.isWhiteList());
        Assert.assertEquals(equatables.getValues().size(), 0);
        Assert.assertEquals(equatables.complement(), EquatableValueSet.none(ID));
        Assert.assertTrue(equatables.containsValue(0L));
        Assert.assertTrue(equatables.containsValue(1L));
    }

    @Test
    public void testSingleValue()
            throws Exception
    {
        EquatableValueSet equatables = EquatableValueSet.of(ID, 10L);

        EquatableValueSet complement = (EquatableValueSet) EquatableValueSet.all(ID).subtract(equatables);

        // Whitelist
        Assert.assertEquals(equatables.getType(), ID);
        Assert.assertFalse(equatables.isNone());
        Assert.assertFalse(equatables.isAll());
        Assert.assertTrue(equatables.isSingleValue());
        Assert.assertTrue(equatables.isWhiteList());
        Assert.assertTrue(Iterables.elementsEqual(equatables.getValues(), ImmutableList.of(10L)));
        Assert.assertEquals(equatables.complement(), complement);
        Assert.assertFalse(equatables.containsValue(0L));
        Assert.assertFalse(equatables.containsValue(1L));
        Assert.assertTrue(equatables.containsValue(10L));

        // Blacklist
        Assert.assertEquals(complement.getType(), ID);
        Assert.assertFalse(complement.isNone());
        Assert.assertFalse(complement.isAll());
        Assert.assertFalse(complement.isSingleValue());
        Assert.assertFalse(complement.isWhiteList());
        Assert.assertTrue(Iterables.elementsEqual(complement.getValues(), ImmutableList.of(10L)));
        Assert.assertEquals(complement.complement(), equatables);
        Assert.assertTrue(complement.containsValue(0L));
        Assert.assertTrue(complement.containsValue(1L));
        Assert.assertFalse(complement.containsValue(10L));
    }

    @Test
    public void testMultipleValues()
            throws Exception
    {
        EquatableValueSet equatables = EquatableValueSet.of(ID, 1L, 2L, 3L, 1L);

        EquatableValueSet complement = (EquatableValueSet) EquatableValueSet.all(ID).subtract(equatables);

        // Whitelist
        Assert.assertEquals(equatables.getType(), ID);
        Assert.assertFalse(equatables.isNone());
        Assert.assertFalse(equatables.isAll());
        Assert.assertFalse(equatables.isSingleValue());
        Assert.assertTrue(equatables.isWhiteList());
        Assert.assertTrue(Iterables.elementsEqual(equatables.getValues(), ImmutableList.of(1L, 2L, 3L)));
        Assert.assertEquals(equatables.complement(), complement);
        Assert.assertFalse(equatables.containsValue(0L));
        Assert.assertTrue(equatables.containsValue(1L));
        Assert.assertTrue(equatables.containsValue(2L));
        Assert.assertTrue(equatables.containsValue(3L));
        Assert.assertFalse(equatables.containsValue(4L));

        // Blacklist
        Assert.assertEquals(complement.getType(), ID);
        Assert.assertFalse(complement.isNone());
        Assert.assertFalse(complement.isAll());
        Assert.assertFalse(complement.isSingleValue());
        Assert.assertFalse(complement.isWhiteList());
        Assert.assertTrue(Iterables.elementsEqual(complement.getValues(), ImmutableList.of(1L, 2L, 3L)));
        Assert.assertEquals(complement.complement(), equatables);
        Assert.assertTrue(complement.containsValue(0L));
        Assert.assertFalse(complement.containsValue(1L));
        Assert.assertFalse(complement.containsValue(2L));
        Assert.assertFalse(complement.containsValue(3L));
        Assert.assertTrue(complement.containsValue(4L));
    }

    @Test
    public void testGetSingleValue()
            throws Exception
    {
        Assert.assertEquals(EquatableValueSet.of(ID, 0L).getSingleValue(), 0L);
        try {
            EquatableValueSet.all(ID).getSingleValue();
            Assert.fail();
        }
        catch (IllegalStateException ignored) {
        }
    }

    @Test
    public void testOverlaps()
            throws Exception
    {
        Assert.assertTrue(EquatableValueSet.all(ID).overlaps(EquatableValueSet.all(ID)));
        Assert.assertFalse(EquatableValueSet.all(ID).overlaps(EquatableValueSet.none(ID)));
        Assert.assertTrue(EquatableValueSet.all(ID).overlaps(EquatableValueSet.of(ID, 0L)));
        Assert.assertTrue(EquatableValueSet.all(ID).overlaps(EquatableValueSet.of(ID, 0L, 1L)));
        Assert.assertTrue(EquatableValueSet.all(ID).overlaps(EquatableValueSet.of(ID, 0L, 1L).complement()));

        Assert.assertFalse(EquatableValueSet.none(ID).overlaps(EquatableValueSet.all(ID)));
        Assert.assertFalse(EquatableValueSet.none(ID).overlaps(EquatableValueSet.none(ID)));
        Assert.assertFalse(EquatableValueSet.none(ID).overlaps(EquatableValueSet.of(ID, 0L)));
        Assert.assertFalse(EquatableValueSet.none(ID).overlaps(EquatableValueSet.of(ID, 0L, 1L)));
        Assert.assertFalse(EquatableValueSet.none(ID).overlaps(EquatableValueSet.of(ID, 0L, 1L).complement()));

        Assert.assertTrue(EquatableValueSet.of(ID, 0L).overlaps(EquatableValueSet.all(ID)));
        Assert.assertFalse(EquatableValueSet.of(ID, 0L).overlaps(EquatableValueSet.none(ID)));
        Assert.assertTrue(EquatableValueSet.of(ID, 0L).overlaps(EquatableValueSet.of(ID, 0L)));
        Assert.assertFalse(EquatableValueSet.of(ID, 0L).overlaps(EquatableValueSet.of(ID, 1L)));
        Assert.assertTrue(EquatableValueSet.of(ID, 0L).overlaps(EquatableValueSet.of(ID, 0L, 1L)));
        Assert.assertFalse(EquatableValueSet.of(ID, 0L).overlaps(EquatableValueSet.of(ID, 0L, 1L).complement()));
        Assert.assertFalse(EquatableValueSet.of(ID, 0L).overlaps(EquatableValueSet.of(ID, 0L).complement()));
        Assert.assertTrue(EquatableValueSet.of(ID, 0L).overlaps(EquatableValueSet.of(ID, 1L).complement()));

        Assert.assertTrue(EquatableValueSet.of(ID, 0L, 1L).overlaps(EquatableValueSet.all(ID)));
        Assert.assertFalse(EquatableValueSet.of(ID, 0L, 1L).overlaps(EquatableValueSet.none(ID)));
        Assert.assertTrue(EquatableValueSet.of(ID, 0L, 1L).overlaps(EquatableValueSet.of(ID, 0L)));
        Assert.assertFalse(EquatableValueSet.of(ID, 0L, 1L).overlaps(EquatableValueSet.of(ID, -1L)));
        Assert.assertTrue(EquatableValueSet.of(ID, 0L, 1L).overlaps(EquatableValueSet.of(ID, 0L, 1L)));
        Assert.assertTrue(EquatableValueSet.of(ID, 0L, 1L).overlaps(EquatableValueSet.of(ID, -1L).complement()));

        Assert.assertTrue(EquatableValueSet.of(ID, 0L, 1L).complement().overlaps(EquatableValueSet.all(ID)));
        Assert.assertFalse(EquatableValueSet.of(ID, 0L, 1L).complement().overlaps(EquatableValueSet.none(ID)));
        Assert.assertFalse(EquatableValueSet.of(ID, 0L, 1L).complement().overlaps(EquatableValueSet.of(ID, 0L)));
        Assert.assertTrue(EquatableValueSet.of(ID, 0L, 1L).complement().overlaps(EquatableValueSet.of(ID, -1L)));
        Assert.assertFalse(EquatableValueSet.of(ID, 0L, 1L).complement().overlaps(EquatableValueSet.of(ID, 0L, 1L)));
        Assert.assertTrue(EquatableValueSet.of(ID, 0L, 1L).complement().overlaps(EquatableValueSet.of(ID, -1L).complement()));
    }

    @Test
    public void testContains()
            throws Exception
    {
        Assert.assertTrue(EquatableValueSet.all(ID).contains(EquatableValueSet.all(ID)));
        Assert.assertTrue(EquatableValueSet.all(ID).contains(EquatableValueSet.none(ID)));
        Assert.assertTrue(EquatableValueSet.all(ID).contains(EquatableValueSet.of(ID, 0L)));
        Assert.assertTrue(EquatableValueSet.all(ID).contains(EquatableValueSet.of(ID, 0L, 1L)));
        Assert.assertTrue(EquatableValueSet.all(ID).contains(EquatableValueSet.of(ID, 0L, 1L).complement()));

        Assert.assertFalse(EquatableValueSet.none(ID).contains(EquatableValueSet.all(ID)));
        Assert.assertTrue(EquatableValueSet.none(ID).contains(EquatableValueSet.none(ID)));
        Assert.assertFalse(EquatableValueSet.none(ID).contains(EquatableValueSet.of(ID, 0L)));
        Assert.assertFalse(EquatableValueSet.none(ID).contains(EquatableValueSet.of(ID, 0L, 1L)));
        Assert.assertFalse(EquatableValueSet.none(ID).contains(EquatableValueSet.of(ID, 0L, 1L).complement()));

        Assert.assertFalse(EquatableValueSet.of(ID, 0L).contains(EquatableValueSet.all(ID)));
        Assert.assertTrue(EquatableValueSet.of(ID, 0L).contains(EquatableValueSet.none(ID)));
        Assert.assertTrue(EquatableValueSet.of(ID, 0L).contains(EquatableValueSet.of(ID, 0L)));
        Assert.assertFalse(EquatableValueSet.of(ID, 0L).contains(EquatableValueSet.of(ID, 0L, 1L)));
        Assert.assertFalse(EquatableValueSet.of(ID, 0L).contains(EquatableValueSet.of(ID, 0L, 1L).complement()));
        Assert.assertFalse(EquatableValueSet.of(ID, 0L).contains(EquatableValueSet.of(ID, 0L).complement()));
        Assert.assertFalse(EquatableValueSet.of(ID, 0L).contains(EquatableValueSet.of(ID, 1L).complement()));

        Assert.assertFalse(EquatableValueSet.of(ID, 0L, 1L).contains(EquatableValueSet.all(ID)));
        Assert.assertTrue(EquatableValueSet.of(ID, 0L, 1L).contains(EquatableValueSet.none(ID)));
        Assert.assertTrue(EquatableValueSet.of(ID, 0L, 1L).contains(EquatableValueSet.of(ID, 0L)));
        Assert.assertTrue(EquatableValueSet.of(ID, 0L, 1L).contains(EquatableValueSet.of(ID, 0L, 1L)));
        Assert.assertFalse(EquatableValueSet.of(ID, 0L, 1L).contains(EquatableValueSet.of(ID, 0L, 2L)));
        Assert.assertFalse(EquatableValueSet.of(ID, 0L, 1L).contains(EquatableValueSet.of(ID, 0L, 1L).complement()));
        Assert.assertFalse(EquatableValueSet.of(ID, 0L, 1L).contains(EquatableValueSet.of(ID, 0L).complement()));
        Assert.assertFalse(EquatableValueSet.of(ID, 0L, 1L).contains(EquatableValueSet.of(ID, 1L).complement()));

        Assert.assertFalse(EquatableValueSet.of(ID, 0L, 1L).complement().contains(EquatableValueSet.all(ID)));
        Assert.assertTrue(EquatableValueSet.of(ID, 0L, 1L).complement().contains(EquatableValueSet.none(ID)));
        Assert.assertFalse(EquatableValueSet.of(ID, 0L, 1L).complement().contains(EquatableValueSet.of(ID, 0L)));
        Assert.assertTrue(EquatableValueSet.of(ID, 0L, 1L).complement().contains(EquatableValueSet.of(ID, -1L)));
        Assert.assertFalse(EquatableValueSet.of(ID, 0L, 1L).complement().contains(EquatableValueSet.of(ID, 0L, 1L)));
        Assert.assertFalse(EquatableValueSet.of(ID, 0L, 1L).complement().contains(EquatableValueSet.of(ID, -1L).complement()));
    }

    @Test
    public void testIntersect()
            throws Exception
    {
        Assert.assertEquals(EquatableValueSet.none(ID).intersect(EquatableValueSet.none(ID)), EquatableValueSet.none(ID));
        Assert.assertEquals(EquatableValueSet.all(ID).intersect(EquatableValueSet.all(ID)), EquatableValueSet.all(ID));
        Assert.assertEquals(EquatableValueSet.none(ID).intersect(EquatableValueSet.all(ID)), EquatableValueSet.none(ID));
        Assert.assertEquals(EquatableValueSet.none(ID).intersect(EquatableValueSet.of(ID, 0L)), EquatableValueSet.none(ID));
        Assert.assertEquals(EquatableValueSet.all(ID).intersect(EquatableValueSet.of(ID, 0L)), EquatableValueSet.of(ID, 0L));
        Assert.assertEquals(EquatableValueSet.of(ID, 0L).intersect(EquatableValueSet.of(ID, 0L)), EquatableValueSet.of(ID, 0L));
        Assert.assertEquals(EquatableValueSet.of(ID, 0L, 1L).intersect(EquatableValueSet.of(ID, 0L)), EquatableValueSet.of(ID, 0L));
        Assert.assertEquals(EquatableValueSet.of(ID, 0L).complement().intersect(EquatableValueSet.of(ID, 0L)), EquatableValueSet.none(ID));
        Assert.assertEquals(EquatableValueSet.of(ID, 0L).complement().intersect(EquatableValueSet.of(ID, 1L)), EquatableValueSet.of(ID, 1L));
        Assert.assertEquals(EquatableValueSet.of(ID, 0L).intersect(EquatableValueSet.of(ID, 1L).complement()), EquatableValueSet.of(ID, 0L));
        Assert.assertEquals(EquatableValueSet.of(ID, 0L, 1L).intersect(EquatableValueSet.of(ID, 0L, 2L)), EquatableValueSet.of(ID, 0L));
        Assert.assertEquals(EquatableValueSet.of(ID, 0L, 1L).complement().intersect(EquatableValueSet.of(ID, 0L, 2L)), EquatableValueSet.of(ID, 2L));
        Assert.assertEquals(EquatableValueSet.of(ID, 0L, 1L).complement().intersect(EquatableValueSet.of(ID, 0L, 2L).complement()), EquatableValueSet.of(ID, 0L, 1L, 2L).complement());
    }

    @Test
    public void testUnion()
            throws Exception
    {
        Assert.assertEquals(EquatableValueSet.none(ID).union(EquatableValueSet.none(ID)), EquatableValueSet.none(ID));
        Assert.assertEquals(EquatableValueSet.all(ID).union(EquatableValueSet.all(ID)), EquatableValueSet.all(ID));
        Assert.assertEquals(EquatableValueSet.none(ID).union(EquatableValueSet.all(ID)), EquatableValueSet.all(ID));
        Assert.assertEquals(EquatableValueSet.none(ID).union(EquatableValueSet.of(ID, 0L)), EquatableValueSet.of(ID, 0L));
        Assert.assertEquals(EquatableValueSet.all(ID).union(EquatableValueSet.of(ID, 0L)), EquatableValueSet.all(ID));
        Assert.assertEquals(EquatableValueSet.of(ID, 0L).union(EquatableValueSet.of(ID, 0L)), EquatableValueSet.of(ID, 0L));
        Assert.assertEquals(EquatableValueSet.of(ID, 0L, 1L).union(EquatableValueSet.of(ID, 0L)), EquatableValueSet.of(ID, 0L, 1L));
        Assert.assertEquals(EquatableValueSet.of(ID, 0L).complement().union(EquatableValueSet.of(ID, 0L)), EquatableValueSet.all(ID));
        Assert.assertEquals(EquatableValueSet.of(ID, 0L).complement().union(EquatableValueSet.of(ID, 1L)), EquatableValueSet.of(ID, 0L).complement());
        Assert.assertEquals(EquatableValueSet.of(ID, 0L).union(EquatableValueSet.of(ID, 1L).complement()), EquatableValueSet.of(ID, 1L).complement());
        Assert.assertEquals(EquatableValueSet.of(ID, 0L, 1L).union(EquatableValueSet.of(ID, 0L, 2L)), EquatableValueSet.of(ID, 0L, 1L, 2L));
        Assert.assertEquals(EquatableValueSet.of(ID, 0L, 1L).complement().union(EquatableValueSet.of(ID, 0L, 2L)), EquatableValueSet.of(ID, 1L).complement());
        Assert.assertEquals(EquatableValueSet.of(ID, 0L, 1L).complement().union(EquatableValueSet.of(ID, 0L, 2L).complement()), EquatableValueSet.of(ID, 0L).complement());
    }

    @Test
    public void testSubtract()
            throws Exception
    {
        Assert.assertEquals(EquatableValueSet.all(ID).subtract(EquatableValueSet.all(ID)), EquatableValueSet.none(ID));
        Assert.assertEquals(EquatableValueSet.all(ID).subtract(EquatableValueSet.none(ID)), EquatableValueSet.all(ID));
        Assert.assertEquals(EquatableValueSet.all(ID).subtract(EquatableValueSet.of(ID, 0L)), EquatableValueSet.of(ID, 0L).complement());
        Assert.assertEquals(EquatableValueSet.all(ID).subtract(EquatableValueSet.of(ID, 0L, 1L)), EquatableValueSet.of(ID, 0L, 1L).complement());
        Assert.assertEquals(EquatableValueSet.all(ID).subtract(EquatableValueSet.of(ID, 0L, 1L).complement()), EquatableValueSet.of(ID, 0L, 1L));

        Assert.assertEquals(EquatableValueSet.none(ID).subtract(EquatableValueSet.all(ID)), EquatableValueSet.none(ID));
        Assert.assertEquals(EquatableValueSet.none(ID).subtract(EquatableValueSet.none(ID)), EquatableValueSet.none(ID));
        Assert.assertEquals(EquatableValueSet.none(ID).subtract(EquatableValueSet.of(ID, 0L)), EquatableValueSet.none(ID));
        Assert.assertEquals(EquatableValueSet.none(ID).subtract(EquatableValueSet.of(ID, 0L, 1L)), EquatableValueSet.none(ID));
        Assert.assertEquals(EquatableValueSet.none(ID).subtract(EquatableValueSet.of(ID, 0L, 1L).complement()), EquatableValueSet.none(ID));

        Assert.assertEquals(EquatableValueSet.of(ID, 0L).subtract(EquatableValueSet.all(ID)), EquatableValueSet.none(ID));
        Assert.assertEquals(EquatableValueSet.of(ID, 0L).subtract(EquatableValueSet.none(ID)), EquatableValueSet.of(ID, 0L));
        Assert.assertEquals(EquatableValueSet.of(ID, 0L).subtract(EquatableValueSet.of(ID, 0L)), EquatableValueSet.none(ID));
        Assert.assertEquals(EquatableValueSet.of(ID, 0L).subtract(EquatableValueSet.of(ID, 0L).complement()), EquatableValueSet.of(ID, 0L));
        Assert.assertEquals(EquatableValueSet.of(ID, 0L).subtract(EquatableValueSet.of(ID, 1L)), EquatableValueSet.of(ID, 0L));
        Assert.assertEquals(EquatableValueSet.of(ID, 0L).subtract(EquatableValueSet.of(ID, 1L).complement()), EquatableValueSet.none(ID));
        Assert.assertEquals(EquatableValueSet.of(ID, 0L).subtract(EquatableValueSet.of(ID, 0L, 1L)), EquatableValueSet.none(ID));
        Assert.assertEquals(EquatableValueSet.of(ID, 0L).subtract(EquatableValueSet.of(ID, 0L, 1L).complement()), EquatableValueSet.of(ID, 0L));

        Assert.assertEquals(EquatableValueSet.of(ID, 0L).complement().subtract(EquatableValueSet.all(ID)), EquatableValueSet.none(ID));
        Assert.assertEquals(EquatableValueSet.of(ID, 0L).complement().subtract(EquatableValueSet.none(ID)), EquatableValueSet.of(ID, 0L).complement());
        Assert.assertEquals(EquatableValueSet.of(ID, 0L).complement().subtract(EquatableValueSet.of(ID, 0L)), EquatableValueSet.of(ID, 0L).complement());
        Assert.assertEquals(EquatableValueSet.of(ID, 0L).complement().subtract(EquatableValueSet.of(ID, 0L).complement()), EquatableValueSet.none(ID));
        Assert.assertEquals(EquatableValueSet.of(ID, 0L).complement().subtract(EquatableValueSet.of(ID, 1L)), EquatableValueSet.of(ID, 0L, 1L).complement());
        Assert.assertEquals(EquatableValueSet.of(ID, 0L).complement().subtract(EquatableValueSet.of(ID, 1L).complement()), EquatableValueSet.of(ID, 1L));
        Assert.assertEquals(EquatableValueSet.of(ID, 0L).complement().subtract(EquatableValueSet.of(ID, 0L, 1L)), EquatableValueSet.of(ID, 0L, 1L).complement());
        Assert.assertEquals(EquatableValueSet.of(ID, 0L).complement().subtract(EquatableValueSet.of(ID, 0L, 1L).complement()), EquatableValueSet.of(ID, 1L));
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testUnmodifiableCollection()
            throws Exception
    {
        EquatableValueSet.of(ID, 1L).getValues().clear();
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testUnmodifiableValueEntries()
            throws Exception
    {
        EquatableValueSet.of(ID, 1L).getEntries().clear();
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testUnmodifiableIterator()
            throws Exception
    {
        Iterator<Object> iterator = EquatableValueSet.of(ID, 1L).getValues().iterator();
        iterator.next();
        iterator.remove();
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testUnmodifiableValueEntryIterator()
            throws Exception
    {
        Iterator<EquatableValueSet.ValueEntry> iterator = EquatableValueSet.of(ID, 1L).getEntries().iterator();
        iterator.next();
        iterator.remove();
    }

    @Test
    public void testJsonSerialization()
            throws Exception
    {
        TestingTypeManager typeManager = new TestingTypeManager();
        TestingBlockEncodingSerde blockEncodingSerde = new TestingBlockEncodingSerde(typeManager);

        ObjectMapper mapper = new ObjectMapperProvider().get()
                .registerModule(new SimpleModule()
                        .addDeserializer(Type.class, new TestingTypeDeserializer(typeManager))
                        .addSerializer(Block.class, new TestingBlockJsonSerde.Serializer(blockEncodingSerde))
                        .addDeserializer(Block.class, new TestingBlockJsonSerde.Deserializer(blockEncodingSerde)));

        EquatableValueSet set = EquatableValueSet.all(ID);
        Assert.assertEquals(set, mapper.readValue(mapper.writeValueAsString(set), EquatableValueSet.class));

        set = EquatableValueSet.none(ID);
        Assert.assertEquals(set, mapper.readValue(mapper.writeValueAsString(set), EquatableValueSet.class));

        set = EquatableValueSet.of(ID, 1L);
        Assert.assertEquals(set, mapper.readValue(mapper.writeValueAsString(set), EquatableValueSet.class));

        set = EquatableValueSet.of(ID, 1L, 2L);
        Assert.assertEquals(set, mapper.readValue(mapper.writeValueAsString(set), EquatableValueSet.class));

        set = EquatableValueSet.of(ID, 1L, 2L).complement();
        Assert.assertEquals(set, mapper.readValue(mapper.writeValueAsString(set), EquatableValueSet.class));
    }
}
