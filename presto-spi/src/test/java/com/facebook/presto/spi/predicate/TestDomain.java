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
import io.airlift.json.ObjectMapperProvider;
import io.airlift.slice.Slices;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static com.facebook.presto.spi.type.TestingIdType.ID;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class TestDomain
{
    @Test
    public void testOrderableNone()
            throws Exception
    {
        Domain domain = Domain.none(BIGINT);
        Assert.assertTrue(domain.isNone());
        Assert.assertFalse(domain.isAll());
        Assert.assertFalse(domain.isSingleValue());
        Assert.assertFalse(domain.isNullableSingleValue());
        Assert.assertFalse(domain.isNullAllowed());
        Assert.assertEquals(domain.getValues(), ValueSet.none(BIGINT));
        Assert.assertEquals(domain.getType(), BIGINT);
        Assert.assertFalse(domain.includesNullableValue(Long.MIN_VALUE));
        Assert.assertFalse(domain.includesNullableValue(0L));
        Assert.assertFalse(domain.includesNullableValue(Long.MAX_VALUE));
        Assert.assertFalse(domain.includesNullableValue(null));
        Assert.assertEquals(domain.complement(), Domain.all(BIGINT));
    }

    @Test
    public void testEquatableNone()
            throws Exception
    {
        Domain domain = Domain.none(ID);
        Assert.assertTrue(domain.isNone());
        Assert.assertFalse(domain.isAll());
        Assert.assertFalse(domain.isSingleValue());
        Assert.assertFalse(domain.isNullableSingleValue());
        Assert.assertFalse(domain.isNullAllowed());
        Assert.assertEquals(domain.getValues(), ValueSet.none(ID));
        Assert.assertEquals(domain.getType(), ID);
        Assert.assertFalse(domain.includesNullableValue(0L));
        Assert.assertFalse(domain.includesNullableValue(null));
        Assert.assertEquals(domain.complement(), Domain.all(ID));
    }

    @Test
    public void testUncomparableNone()
            throws Exception
    {
        Domain domain = Domain.none(HYPER_LOG_LOG);
        Assert.assertTrue(domain.isNone());
        Assert.assertFalse(domain.isAll());
        Assert.assertFalse(domain.isSingleValue());
        Assert.assertFalse(domain.isNullableSingleValue());
        Assert.assertFalse(domain.isNullAllowed());
        Assert.assertEquals(domain.getValues(), ValueSet.none(HYPER_LOG_LOG));
        Assert.assertEquals(domain.getType(), HYPER_LOG_LOG);
        Assert.assertFalse(domain.includesNullableValue(Slices.EMPTY_SLICE));
        Assert.assertFalse(domain.includesNullableValue(null));
        Assert.assertEquals(domain.complement(), Domain.all(HYPER_LOG_LOG));
    }

    @Test
    public void testOrderableAll()
            throws Exception
    {
        Domain domain = Domain.all(BIGINT);
        Assert.assertFalse(domain.isNone());
        Assert.assertTrue(domain.isAll());
        Assert.assertFalse(domain.isSingleValue());
        Assert.assertFalse(domain.isNullableSingleValue());
        Assert.assertFalse(domain.isOnlyNull());
        Assert.assertTrue(domain.isNullAllowed());
        Assert.assertEquals(domain.getValues(), ValueSet.all(BIGINT));
        Assert.assertEquals(domain.getType(), BIGINT);
        Assert.assertTrue(domain.includesNullableValue(Long.MIN_VALUE));
        Assert.assertTrue(domain.includesNullableValue(0L));
        Assert.assertTrue(domain.includesNullableValue(Long.MAX_VALUE));
        Assert.assertTrue(domain.includesNullableValue(null));
        Assert.assertEquals(domain.complement(), Domain.none(BIGINT));
    }

    @Test
    public void testEquatableAll()
            throws Exception
    {
        Domain domain = Domain.all(ID);
        Assert.assertFalse(domain.isNone());
        Assert.assertTrue(domain.isAll());
        Assert.assertFalse(domain.isSingleValue());
        Assert.assertFalse(domain.isNullableSingleValue());
        Assert.assertFalse(domain.isOnlyNull());
        Assert.assertTrue(domain.isNullAllowed());
        Assert.assertEquals(domain.getValues(), ValueSet.all(ID));
        Assert.assertEquals(domain.getType(), ID);
        Assert.assertTrue(domain.includesNullableValue(0L));
        Assert.assertTrue(domain.includesNullableValue(null));
        Assert.assertEquals(domain.complement(), Domain.none(ID));
    }

    @Test
    public void testUncomparableAll()
            throws Exception
    {
        Domain domain = Domain.all(HYPER_LOG_LOG);
        Assert.assertFalse(domain.isNone());
        Assert.assertTrue(domain.isAll());
        Assert.assertFalse(domain.isSingleValue());
        Assert.assertFalse(domain.isNullableSingleValue());
        Assert.assertFalse(domain.isOnlyNull());
        Assert.assertTrue(domain.isNullAllowed());
        Assert.assertEquals(domain.getValues(), ValueSet.all(HYPER_LOG_LOG));
        Assert.assertEquals(domain.getType(), HYPER_LOG_LOG);
        Assert.assertTrue(domain.includesNullableValue(Slices.EMPTY_SLICE));
        Assert.assertTrue(domain.includesNullableValue(null));
        Assert.assertEquals(domain.complement(), Domain.none(HYPER_LOG_LOG));
    }

    @Test
    public void testOrderableNullOnly()
            throws Exception
    {
        Domain domain = Domain.onlyNull(BIGINT);
        Assert.assertFalse(domain.isNone());
        Assert.assertFalse(domain.isAll());
        Assert.assertFalse(domain.isSingleValue());
        Assert.assertTrue(domain.isNullAllowed());
        Assert.assertTrue(domain.isNullableSingleValue());
        Assert.assertTrue(domain.isOnlyNull());
        Assert.assertEquals(domain.getValues(), ValueSet.none(BIGINT));
        Assert.assertEquals(domain.getType(), BIGINT);
        Assert.assertFalse(domain.includesNullableValue(Long.MIN_VALUE));
        Assert.assertFalse(domain.includesNullableValue(0L));
        Assert.assertFalse(domain.includesNullableValue(Long.MAX_VALUE));
        Assert.assertTrue(domain.includesNullableValue(null));
        Assert.assertEquals(domain.complement(), Domain.notNull(BIGINT));
        Assert.assertEquals(domain.getNullableSingleValue(), null);
    }

    @Test
    public void testEquatableNullOnly()
            throws Exception
    {
        Domain domain = Domain.onlyNull(ID);
        Assert.assertFalse(domain.isNone());
        Assert.assertFalse(domain.isAll());
        Assert.assertFalse(domain.isSingleValue());
        Assert.assertTrue(domain.isNullableSingleValue());
        Assert.assertTrue(domain.isOnlyNull());
        Assert.assertTrue(domain.isNullAllowed());
        Assert.assertEquals(domain.getValues(), ValueSet.none(ID));
        Assert.assertEquals(domain.getType(), ID);
        Assert.assertFalse(domain.includesNullableValue(0L));
        Assert.assertTrue(domain.includesNullableValue(null));
        Assert.assertEquals(domain.complement(), Domain.notNull(ID));
        Assert.assertEquals(domain.getNullableSingleValue(), null);
    }

    @Test
    public void testUncomparableNullOnly()
            throws Exception
    {
        Domain domain = Domain.onlyNull(HYPER_LOG_LOG);
        Assert.assertFalse(domain.isNone());
        Assert.assertFalse(domain.isAll());
        Assert.assertFalse(domain.isSingleValue());
        Assert.assertTrue(domain.isNullableSingleValue());
        Assert.assertTrue(domain.isOnlyNull());
        Assert.assertTrue(domain.isNullAllowed());
        Assert.assertEquals(domain.getValues(), ValueSet.none(HYPER_LOG_LOG));
        Assert.assertEquals(domain.getType(), HYPER_LOG_LOG);
        Assert.assertFalse(domain.includesNullableValue(Slices.EMPTY_SLICE));
        Assert.assertTrue(domain.includesNullableValue(null));
        Assert.assertEquals(domain.complement(), Domain.notNull(HYPER_LOG_LOG));
        Assert.assertEquals(domain.getNullableSingleValue(), null);
    }

    @Test
    public void testOrderableNotNull()
            throws Exception
    {
        Domain domain = Domain.notNull(BIGINT);
        Assert.assertFalse(domain.isNone());
        Assert.assertFalse(domain.isAll());
        Assert.assertFalse(domain.isSingleValue());
        Assert.assertFalse(domain.isNullableSingleValue());
        Assert.assertFalse(domain.isOnlyNull());
        Assert.assertFalse(domain.isNullAllowed());
        Assert.assertEquals(domain.getValues(), ValueSet.all(BIGINT));
        Assert.assertEquals(domain.getType(), BIGINT);
        Assert.assertTrue(domain.includesNullableValue(Long.MIN_VALUE));
        Assert.assertTrue(domain.includesNullableValue(0L));
        Assert.assertTrue(domain.includesNullableValue(Long.MAX_VALUE));
        Assert.assertFalse(domain.includesNullableValue(null));
        Assert.assertEquals(domain.complement(), Domain.onlyNull(BIGINT));
    }

    @Test
    public void testEquatableNotNull()
            throws Exception
    {
        Domain domain = Domain.notNull(ID);
        Assert.assertFalse(domain.isNone());
        Assert.assertFalse(domain.isAll());
        Assert.assertFalse(domain.isSingleValue());
        Assert.assertFalse(domain.isNullableSingleValue());
        Assert.assertFalse(domain.isOnlyNull());
        Assert.assertFalse(domain.isNullAllowed());
        Assert.assertEquals(domain.getValues(), ValueSet.all(ID));
        Assert.assertEquals(domain.getType(), ID);
        Assert.assertTrue(domain.includesNullableValue(0L));
        Assert.assertFalse(domain.includesNullableValue(null));
        Assert.assertEquals(domain.complement(), Domain.onlyNull(ID));
    }

    @Test
    public void testUncomparableNotNull()
            throws Exception
    {
        Domain domain = Domain.notNull(HYPER_LOG_LOG);
        Assert.assertFalse(domain.isNone());
        Assert.assertFalse(domain.isAll());
        Assert.assertFalse(domain.isSingleValue());
        Assert.assertFalse(domain.isNullableSingleValue());
        Assert.assertFalse(domain.isOnlyNull());
        Assert.assertFalse(domain.isNullAllowed());
        Assert.assertEquals(domain.getValues(), ValueSet.all(HYPER_LOG_LOG));
        Assert.assertEquals(domain.getType(), HYPER_LOG_LOG);
        Assert.assertTrue(domain.includesNullableValue(Slices.EMPTY_SLICE));
        Assert.assertFalse(domain.includesNullableValue(null));
        Assert.assertEquals(domain.complement(), Domain.onlyNull(HYPER_LOG_LOG));
    }

    @Test
    public void testOrderableSingleValue()
            throws Exception
    {
        Domain domain = Domain.singleValue(BIGINT, 0L);
        Assert.assertFalse(domain.isNone());
        Assert.assertFalse(domain.isAll());
        Assert.assertTrue(domain.isSingleValue());
        Assert.assertTrue(domain.isNullableSingleValue());
        Assert.assertFalse(domain.isOnlyNull());
        Assert.assertFalse(domain.isNullAllowed());
        Assert.assertEquals(domain.getValues(), ValueSet.ofRanges(Range.equal(BIGINT, 0L)));
        Assert.assertEquals(domain.getType(), BIGINT);
        Assert.assertFalse(domain.includesNullableValue(Long.MIN_VALUE));
        Assert.assertTrue(domain.includesNullableValue(0L));
        Assert.assertFalse(domain.includesNullableValue(Long.MAX_VALUE));
        Assert.assertEquals(domain.complement(), Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 0L), Range.greaterThan(BIGINT, 0L)), true));
        Assert.assertEquals(domain.getSingleValue(), 0L);
        Assert.assertEquals(domain.getNullableSingleValue(), 0L);

        try {
            Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 1L, true, 2L, true)), false).getSingleValue();
            Assert.fail();
        }
        catch (IllegalStateException e) {
        }
    }

    @Test
    public void testEquatableSingleValue()
            throws Exception
    {
        Domain domain = Domain.singleValue(ID, 0L);
        Assert.assertFalse(domain.isNone());
        Assert.assertFalse(domain.isAll());
        Assert.assertTrue(domain.isSingleValue());
        Assert.assertTrue(domain.isNullableSingleValue());
        Assert.assertFalse(domain.isOnlyNull());
        Assert.assertFalse(domain.isNullAllowed());
        Assert.assertEquals(domain.getValues(), ValueSet.of(ID, 0L));
        Assert.assertEquals(domain.getType(), ID);
        Assert.assertTrue(domain.includesNullableValue(0L));
        Assert.assertFalse(domain.includesNullableValue(null));
        Assert.assertEquals(domain.complement(), Domain.create(ValueSet.of(ID, 0L).complement(), true));
        Assert.assertEquals(domain.getSingleValue(), 0L);
        Assert.assertEquals(domain.getNullableSingleValue(), 0L);

        try {
            Domain.create(ValueSet.of(ID, 0L, 1L), false).getSingleValue();
            Assert.fail();
        }
        catch (IllegalStateException e) {
        }
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testUncomparableSingleValue()
            throws Exception
    {
        Domain.singleValue(HYPER_LOG_LOG, Slices.EMPTY_SLICE);
    }

    @Test
    public void testOverlaps()
            throws Exception
    {
        Assert.assertTrue(Domain.all(BIGINT).overlaps(Domain.all(BIGINT)));
        Assert.assertFalse(Domain.all(BIGINT).overlaps(Domain.none(BIGINT)));
        Assert.assertTrue(Domain.all(BIGINT).overlaps(Domain.notNull(BIGINT)));
        Assert.assertTrue(Domain.all(BIGINT).overlaps(Domain.onlyNull(BIGINT)));
        Assert.assertTrue(Domain.all(BIGINT).overlaps(Domain.singleValue(BIGINT, 0L)));

        Assert.assertFalse(Domain.none(BIGINT).overlaps(Domain.all(BIGINT)));
        Assert.assertFalse(Domain.none(BIGINT).overlaps(Domain.none(BIGINT)));
        Assert.assertFalse(Domain.none(BIGINT).overlaps(Domain.notNull(BIGINT)));
        Assert.assertFalse(Domain.none(BIGINT).overlaps(Domain.onlyNull(BIGINT)));
        Assert.assertFalse(Domain.none(BIGINT).overlaps(Domain.singleValue(BIGINT, 0L)));

        Assert.assertTrue(Domain.notNull(BIGINT).overlaps(Domain.all(BIGINT)));
        Assert.assertFalse(Domain.notNull(BIGINT).overlaps(Domain.none(BIGINT)));
        Assert.assertTrue(Domain.notNull(BIGINT).overlaps(Domain.notNull(BIGINT)));
        Assert.assertFalse(Domain.notNull(BIGINT).overlaps(Domain.onlyNull(BIGINT)));
        Assert.assertTrue(Domain.notNull(BIGINT).overlaps(Domain.singleValue(BIGINT, 0L)));

        Assert.assertTrue(Domain.onlyNull(BIGINT).overlaps(Domain.all(BIGINT)));
        Assert.assertFalse(Domain.onlyNull(BIGINT).overlaps(Domain.none(BIGINT)));
        Assert.assertFalse(Domain.onlyNull(BIGINT).overlaps(Domain.notNull(BIGINT)));
        Assert.assertTrue(Domain.onlyNull(BIGINT).overlaps(Domain.onlyNull(BIGINT)));
        Assert.assertFalse(Domain.onlyNull(BIGINT).overlaps(Domain.singleValue(BIGINT, 0L)));

        Assert.assertTrue(Domain.singleValue(BIGINT, 0L).overlaps(Domain.all(BIGINT)));
        Assert.assertFalse(Domain.singleValue(BIGINT, 0L).overlaps(Domain.none(BIGINT)));
        Assert.assertTrue(Domain.singleValue(BIGINT, 0L).overlaps(Domain.notNull(BIGINT)));
        Assert.assertFalse(Domain.singleValue(BIGINT, 0L).overlaps(Domain.onlyNull(BIGINT)));
        Assert.assertTrue(Domain.singleValue(BIGINT, 0L).overlaps(Domain.singleValue(BIGINT, 0L)));
    }

    @Test
    public void testContains()
            throws Exception
    {
        Assert.assertTrue(Domain.all(BIGINT).contains(Domain.all(BIGINT)));
        Assert.assertTrue(Domain.all(BIGINT).contains(Domain.none(BIGINT)));
        Assert.assertTrue(Domain.all(BIGINT).contains(Domain.notNull(BIGINT)));
        Assert.assertTrue(Domain.all(BIGINT).contains(Domain.onlyNull(BIGINT)));
        Assert.assertTrue(Domain.all(BIGINT).contains(Domain.singleValue(BIGINT, 0L)));

        Assert.assertFalse(Domain.none(BIGINT).contains(Domain.all(BIGINT)));
        Assert.assertTrue(Domain.none(BIGINT).contains(Domain.none(BIGINT)));
        Assert.assertFalse(Domain.none(BIGINT).contains(Domain.notNull(BIGINT)));
        Assert.assertFalse(Domain.none(BIGINT).contains(Domain.onlyNull(BIGINT)));
        Assert.assertFalse(Domain.none(BIGINT).contains(Domain.singleValue(BIGINT, 0L)));

        Assert.assertFalse(Domain.notNull(BIGINT).contains(Domain.all(BIGINT)));
        Assert.assertTrue(Domain.notNull(BIGINT).contains(Domain.none(BIGINT)));
        Assert.assertTrue(Domain.notNull(BIGINT).contains(Domain.notNull(BIGINT)));
        Assert.assertFalse(Domain.notNull(BIGINT).contains(Domain.onlyNull(BIGINT)));
        Assert.assertTrue(Domain.notNull(BIGINT).contains(Domain.singleValue(BIGINT, 0L)));

        Assert.assertFalse(Domain.onlyNull(BIGINT).contains(Domain.all(BIGINT)));
        Assert.assertTrue(Domain.onlyNull(BIGINT).contains(Domain.none(BIGINT)));
        Assert.assertFalse(Domain.onlyNull(BIGINT).contains(Domain.notNull(BIGINT)));
        Assert.assertTrue(Domain.onlyNull(BIGINT).contains(Domain.onlyNull(BIGINT)));
        Assert.assertFalse(Domain.onlyNull(BIGINT).contains(Domain.singleValue(BIGINT, 0L)));

        Assert.assertFalse(Domain.singleValue(BIGINT, 0L).contains(Domain.all(BIGINT)));
        Assert.assertTrue(Domain.singleValue(BIGINT, 0L).contains(Domain.none(BIGINT)));
        Assert.assertFalse(Domain.singleValue(BIGINT, 0L).contains(Domain.notNull(BIGINT)));
        Assert.assertFalse(Domain.singleValue(BIGINT, 0L).contains(Domain.onlyNull(BIGINT)));
        Assert.assertTrue(Domain.singleValue(BIGINT, 0L).contains(Domain.singleValue(BIGINT, 0L)));
    }

    @Test
    public void testIntersect()
            throws Exception
    {
        Assert.assertEquals(
                Domain.all(BIGINT).intersect(Domain.all(BIGINT)),
                Domain.all(BIGINT));

        Assert.assertEquals(
                Domain.none(BIGINT).intersect(Domain.none(BIGINT)),
                Domain.none(BIGINT));

        Assert.assertEquals(
                Domain.all(BIGINT).intersect(Domain.none(BIGINT)),
                Domain.none(BIGINT));

        Assert.assertEquals(
                Domain.notNull(BIGINT).intersect(Domain.onlyNull(BIGINT)),
                Domain.none(BIGINT));

        Assert.assertEquals(
                Domain.singleValue(BIGINT, 0L).intersect(Domain.all(BIGINT)),
                Domain.singleValue(BIGINT, 0L));

        Assert.assertEquals(
                Domain.singleValue(BIGINT, 0L).intersect(Domain.onlyNull(BIGINT)),
                Domain.none(BIGINT));

        Assert.assertEquals(
                Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L)), true).intersect(Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 2L)), true)),
                Domain.onlyNull(BIGINT));

        Assert.assertEquals(
                Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L)), true).intersect(Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L)), false)),
                Domain.singleValue(BIGINT, 1L));
    }

    @Test
    public void testUnion()
            throws Exception
    {
        assertUnion(Domain.all(BIGINT), Domain.all(BIGINT), Domain.all(BIGINT));
        assertUnion(Domain.none(BIGINT), Domain.none(BIGINT), Domain.none(BIGINT));
        assertUnion(Domain.all(BIGINT), Domain.none(BIGINT), Domain.all(BIGINT));
        assertUnion(Domain.notNull(BIGINT), Domain.onlyNull(BIGINT), Domain.all(BIGINT));
        assertUnion(Domain.singleValue(BIGINT, 0L), Domain.all(BIGINT), Domain.all(BIGINT));
        assertUnion(Domain.singleValue(BIGINT, 0L), Domain.notNull(BIGINT), Domain.notNull(BIGINT));
        assertUnion(Domain.singleValue(BIGINT, 0L), Domain.onlyNull(BIGINT), Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 0L)), true));

        assertUnion(Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L)), true),
                Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 2L)), true),
                Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L)), true));

        assertUnion(Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L)), true),
                Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L)), false),
                Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L)), true));
    }

    @Test
    public void testSubtract()
            throws Exception
    {
        Assert.assertEquals(
                Domain.all(BIGINT).subtract(Domain.all(BIGINT)),
                Domain.none(BIGINT));
        Assert.assertEquals(
                Domain.all(BIGINT).subtract(Domain.none(BIGINT)),
                Domain.all(BIGINT));
        Assert.assertEquals(
                Domain.all(BIGINT).subtract(Domain.notNull(BIGINT)),
                Domain.onlyNull(BIGINT));
        Assert.assertEquals(
                Domain.all(BIGINT).subtract(Domain.onlyNull(BIGINT)),
                Domain.notNull(BIGINT));
        Assert.assertEquals(
                Domain.all(BIGINT).subtract(Domain.singleValue(BIGINT, 0L)),
                Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 0L), Range.greaterThan(BIGINT, 0L)), true));

        Assert.assertEquals(
                Domain.none(BIGINT).subtract(Domain.all(BIGINT)),
                Domain.none(BIGINT));
        Assert.assertEquals(
                Domain.none(BIGINT).subtract(Domain.none(BIGINT)),
                Domain.none(BIGINT));
        Assert.assertEquals(
                Domain.none(BIGINT).subtract(Domain.notNull(BIGINT)),
                Domain.none(BIGINT));
        Assert.assertEquals(
                Domain.none(BIGINT).subtract(Domain.onlyNull(BIGINT)),
                Domain.none(BIGINT));
        Assert.assertEquals(
                Domain.none(BIGINT).subtract(Domain.singleValue(BIGINT, 0L)),
                Domain.none(BIGINT));

        Assert.assertEquals(
                Domain.notNull(BIGINT).subtract(Domain.all(BIGINT)),
                Domain.none(BIGINT));
        Assert.assertEquals(
                Domain.notNull(BIGINT).subtract(Domain.none(BIGINT)),
                Domain.notNull(BIGINT));
        Assert.assertEquals(
                Domain.notNull(BIGINT).subtract(Domain.notNull(BIGINT)),
                Domain.none(BIGINT));
        Assert.assertEquals(
                Domain.notNull(BIGINT).subtract(Domain.onlyNull(BIGINT)),
                Domain.notNull(BIGINT));
        Assert.assertEquals(
                Domain.notNull(BIGINT).subtract(Domain.singleValue(BIGINT, 0L)),
                Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 0L), Range.greaterThan(BIGINT, 0L)), false));

        Assert.assertEquals(
                Domain.onlyNull(BIGINT).subtract(Domain.all(BIGINT)),
                Domain.none(BIGINT));
        Assert.assertEquals(
                Domain.onlyNull(BIGINT).subtract(Domain.none(BIGINT)),
                Domain.onlyNull(BIGINT));
        Assert.assertEquals(
                Domain.onlyNull(BIGINT).subtract(Domain.notNull(BIGINT)),
                Domain.onlyNull(BIGINT));
        Assert.assertEquals(
                Domain.onlyNull(BIGINT).subtract(Domain.onlyNull(BIGINT)),
                Domain.none(BIGINT));
        Assert.assertEquals(
                Domain.onlyNull(BIGINT).subtract(Domain.singleValue(BIGINT, 0L)),
                Domain.onlyNull(BIGINT));

        Assert.assertEquals(
                Domain.singleValue(BIGINT, 0L).subtract(Domain.all(BIGINT)),
                Domain.none(BIGINT));
        Assert.assertEquals(
                Domain.singleValue(BIGINT, 0L).subtract(Domain.none(BIGINT)),
                Domain.singleValue(BIGINT, 0L));
        Assert.assertEquals(
                Domain.singleValue(BIGINT, 0L).subtract(Domain.notNull(BIGINT)),
                Domain.none(BIGINT));
        Assert.assertEquals(
                Domain.singleValue(BIGINT, 0L).subtract(Domain.onlyNull(BIGINT)),
                Domain.singleValue(BIGINT, 0L));
        Assert.assertEquals(
                Domain.singleValue(BIGINT, 0L).subtract(Domain.singleValue(BIGINT, 0L)),
                Domain.none(BIGINT));

        Assert.assertEquals(
                Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L)), true).subtract(Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 2L)), true)),
                Domain.singleValue(BIGINT, 1L));

        Assert.assertEquals(
                Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L)), true).subtract(Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L)), false)),
                Domain.onlyNull(BIGINT));
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

        Domain domain = Domain.all(BIGINT);
        Assert.assertEquals(domain, mapper.readValue(mapper.writeValueAsString(domain), Domain.class));

        domain = Domain.none(DOUBLE);
        Assert.assertEquals(domain, mapper.readValue(mapper.writeValueAsString(domain), Domain.class));

        domain = Domain.notNull(BOOLEAN);
        Assert.assertEquals(domain, mapper.readValue(mapper.writeValueAsString(domain), Domain.class));

        domain = Domain.notNull(HYPER_LOG_LOG);
        Assert.assertEquals(domain, mapper.readValue(mapper.writeValueAsString(domain), Domain.class));

        domain = Domain.onlyNull(VARCHAR);
        Assert.assertEquals(domain, mapper.readValue(mapper.writeValueAsString(domain), Domain.class));

        domain = Domain.onlyNull(HYPER_LOG_LOG);
        Assert.assertEquals(domain, mapper.readValue(mapper.writeValueAsString(domain), Domain.class));

        domain = Domain.singleValue(BIGINT, Long.MIN_VALUE);
        Assert.assertEquals(domain, mapper.readValue(mapper.writeValueAsString(domain), Domain.class));

        domain = Domain.singleValue(ID, Long.MIN_VALUE);
        Assert.assertEquals(domain, mapper.readValue(mapper.writeValueAsString(domain), Domain.class));

        domain = Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 0L), Range.equal(BIGINT, 1L), Range.range(BIGINT, 2L, true, 3L, true)), true);
        Assert.assertEquals(domain, mapper.readValue(mapper.writeValueAsString(domain), Domain.class));
    }

    private void assertUnion(Domain first, Domain second, Domain expected)
    {
        Assert.assertEquals(first.union(second), expected);
        Assert.assertEquals(Domain.union(ImmutableList.of(first, second)), expected);
    }
}
