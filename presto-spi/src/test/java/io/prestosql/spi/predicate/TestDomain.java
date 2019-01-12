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
import com.google.common.collect.ImmutableList;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.slice.Slices;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.TestingBlockEncodingSerde;
import io.prestosql.spi.block.TestingBlockJsonSerde;
import io.prestosql.spi.type.TestingTypeDeserializer;
import io.prestosql.spi.type.TestingTypeManager;
import io.prestosql.spi.type.Type;
import org.testng.annotations.Test;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static io.prestosql.spi.type.TestingIdType.ID;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestDomain
{
    @Test
    public void testOrderableNone()
    {
        Domain domain = Domain.none(BIGINT);
        assertTrue(domain.isNone());
        assertFalse(domain.isAll());
        assertFalse(domain.isSingleValue());
        assertFalse(domain.isNullableSingleValue());
        assertFalse(domain.isNullAllowed());
        assertEquals(domain.getValues(), ValueSet.none(BIGINT));
        assertEquals(domain.getType(), BIGINT);
        assertFalse(domain.includesNullableValue(Long.MIN_VALUE));
        assertFalse(domain.includesNullableValue(0L));
        assertFalse(domain.includesNullableValue(Long.MAX_VALUE));
        assertFalse(domain.includesNullableValue(null));
        assertEquals(domain.complement(), Domain.all(BIGINT));
    }

    @Test
    public void testEquatableNone()
    {
        Domain domain = Domain.none(ID);
        assertTrue(domain.isNone());
        assertFalse(domain.isAll());
        assertFalse(domain.isSingleValue());
        assertFalse(domain.isNullableSingleValue());
        assertFalse(domain.isNullAllowed());
        assertEquals(domain.getValues(), ValueSet.none(ID));
        assertEquals(domain.getType(), ID);
        assertFalse(domain.includesNullableValue(0L));
        assertFalse(domain.includesNullableValue(null));
        assertEquals(domain.complement(), Domain.all(ID));
    }

    @Test
    public void testUncomparableNone()
    {
        Domain domain = Domain.none(HYPER_LOG_LOG);
        assertTrue(domain.isNone());
        assertFalse(domain.isAll());
        assertFalse(domain.isSingleValue());
        assertFalse(domain.isNullableSingleValue());
        assertFalse(domain.isNullAllowed());
        assertEquals(domain.getValues(), ValueSet.none(HYPER_LOG_LOG));
        assertEquals(domain.getType(), HYPER_LOG_LOG);
        assertFalse(domain.includesNullableValue(Slices.EMPTY_SLICE));
        assertFalse(domain.includesNullableValue(null));
        assertEquals(domain.complement(), Domain.all(HYPER_LOG_LOG));
    }

    @Test
    public void testOrderableAll()
    {
        Domain domain = Domain.all(BIGINT);
        assertFalse(domain.isNone());
        assertTrue(domain.isAll());
        assertFalse(domain.isSingleValue());
        assertFalse(domain.isNullableSingleValue());
        assertFalse(domain.isOnlyNull());
        assertTrue(domain.isNullAllowed());
        assertEquals(domain.getValues(), ValueSet.all(BIGINT));
        assertEquals(domain.getType(), BIGINT);
        assertTrue(domain.includesNullableValue(Long.MIN_VALUE));
        assertTrue(domain.includesNullableValue(0L));
        assertTrue(domain.includesNullableValue(Long.MAX_VALUE));
        assertTrue(domain.includesNullableValue(null));
        assertEquals(domain.complement(), Domain.none(BIGINT));
    }

    @Test
    public void testEquatableAll()
    {
        Domain domain = Domain.all(ID);
        assertFalse(domain.isNone());
        assertTrue(domain.isAll());
        assertFalse(domain.isSingleValue());
        assertFalse(domain.isNullableSingleValue());
        assertFalse(domain.isOnlyNull());
        assertTrue(domain.isNullAllowed());
        assertEquals(domain.getValues(), ValueSet.all(ID));
        assertEquals(domain.getType(), ID);
        assertTrue(domain.includesNullableValue(0L));
        assertTrue(domain.includesNullableValue(null));
        assertEquals(domain.complement(), Domain.none(ID));
    }

    @Test
    public void testUncomparableAll()
    {
        Domain domain = Domain.all(HYPER_LOG_LOG);
        assertFalse(domain.isNone());
        assertTrue(domain.isAll());
        assertFalse(domain.isSingleValue());
        assertFalse(domain.isNullableSingleValue());
        assertFalse(domain.isOnlyNull());
        assertTrue(domain.isNullAllowed());
        assertEquals(domain.getValues(), ValueSet.all(HYPER_LOG_LOG));
        assertEquals(domain.getType(), HYPER_LOG_LOG);
        assertTrue(domain.includesNullableValue(Slices.EMPTY_SLICE));
        assertTrue(domain.includesNullableValue(null));
        assertEquals(domain.complement(), Domain.none(HYPER_LOG_LOG));
    }

    @Test
    public void testOrderableNullOnly()
    {
        Domain domain = Domain.onlyNull(BIGINT);
        assertFalse(domain.isNone());
        assertFalse(domain.isAll());
        assertFalse(domain.isSingleValue());
        assertTrue(domain.isNullAllowed());
        assertTrue(domain.isNullableSingleValue());
        assertTrue(domain.isOnlyNull());
        assertEquals(domain.getValues(), ValueSet.none(BIGINT));
        assertEquals(domain.getType(), BIGINT);
        assertFalse(domain.includesNullableValue(Long.MIN_VALUE));
        assertFalse(domain.includesNullableValue(0L));
        assertFalse(domain.includesNullableValue(Long.MAX_VALUE));
        assertTrue(domain.includesNullableValue(null));
        assertEquals(domain.complement(), Domain.notNull(BIGINT));
        assertEquals(domain.getNullableSingleValue(), null);
    }

    @Test
    public void testEquatableNullOnly()
    {
        Domain domain = Domain.onlyNull(ID);
        assertFalse(domain.isNone());
        assertFalse(domain.isAll());
        assertFalse(domain.isSingleValue());
        assertTrue(domain.isNullableSingleValue());
        assertTrue(domain.isOnlyNull());
        assertTrue(domain.isNullAllowed());
        assertEquals(domain.getValues(), ValueSet.none(ID));
        assertEquals(domain.getType(), ID);
        assertFalse(domain.includesNullableValue(0L));
        assertTrue(domain.includesNullableValue(null));
        assertEquals(domain.complement(), Domain.notNull(ID));
        assertEquals(domain.getNullableSingleValue(), null);
    }

    @Test
    public void testUncomparableNullOnly()
    {
        Domain domain = Domain.onlyNull(HYPER_LOG_LOG);
        assertFalse(domain.isNone());
        assertFalse(domain.isAll());
        assertFalse(domain.isSingleValue());
        assertTrue(domain.isNullableSingleValue());
        assertTrue(domain.isOnlyNull());
        assertTrue(domain.isNullAllowed());
        assertEquals(domain.getValues(), ValueSet.none(HYPER_LOG_LOG));
        assertEquals(domain.getType(), HYPER_LOG_LOG);
        assertFalse(domain.includesNullableValue(Slices.EMPTY_SLICE));
        assertTrue(domain.includesNullableValue(null));
        assertEquals(domain.complement(), Domain.notNull(HYPER_LOG_LOG));
        assertEquals(domain.getNullableSingleValue(), null);
    }

    @Test
    public void testOrderableNotNull()
    {
        Domain domain = Domain.notNull(BIGINT);
        assertFalse(domain.isNone());
        assertFalse(domain.isAll());
        assertFalse(domain.isSingleValue());
        assertFalse(domain.isNullableSingleValue());
        assertFalse(domain.isOnlyNull());
        assertFalse(domain.isNullAllowed());
        assertEquals(domain.getValues(), ValueSet.all(BIGINT));
        assertEquals(domain.getType(), BIGINT);
        assertTrue(domain.includesNullableValue(Long.MIN_VALUE));
        assertTrue(domain.includesNullableValue(0L));
        assertTrue(domain.includesNullableValue(Long.MAX_VALUE));
        assertFalse(domain.includesNullableValue(null));
        assertEquals(domain.complement(), Domain.onlyNull(BIGINT));
    }

    @Test
    public void testEquatableNotNull()
    {
        Domain domain = Domain.notNull(ID);
        assertFalse(domain.isNone());
        assertFalse(domain.isAll());
        assertFalse(domain.isSingleValue());
        assertFalse(domain.isNullableSingleValue());
        assertFalse(domain.isOnlyNull());
        assertFalse(domain.isNullAllowed());
        assertEquals(domain.getValues(), ValueSet.all(ID));
        assertEquals(domain.getType(), ID);
        assertTrue(domain.includesNullableValue(0L));
        assertFalse(domain.includesNullableValue(null));
        assertEquals(domain.complement(), Domain.onlyNull(ID));
    }

    @Test
    public void testUncomparableNotNull()
    {
        Domain domain = Domain.notNull(HYPER_LOG_LOG);
        assertFalse(domain.isNone());
        assertFalse(domain.isAll());
        assertFalse(domain.isSingleValue());
        assertFalse(domain.isNullableSingleValue());
        assertFalse(domain.isOnlyNull());
        assertFalse(domain.isNullAllowed());
        assertEquals(domain.getValues(), ValueSet.all(HYPER_LOG_LOG));
        assertEquals(domain.getType(), HYPER_LOG_LOG);
        assertTrue(domain.includesNullableValue(Slices.EMPTY_SLICE));
        assertFalse(domain.includesNullableValue(null));
        assertEquals(domain.complement(), Domain.onlyNull(HYPER_LOG_LOG));
    }

    @Test
    public void testOrderableSingleValue()
    {
        Domain domain = Domain.singleValue(BIGINT, 0L);
        assertFalse(domain.isNone());
        assertFalse(domain.isAll());
        assertTrue(domain.isSingleValue());
        assertTrue(domain.isNullableSingleValue());
        assertFalse(domain.isOnlyNull());
        assertFalse(domain.isNullAllowed());
        assertEquals(domain.getValues(), ValueSet.ofRanges(Range.equal(BIGINT, 0L)));
        assertEquals(domain.getType(), BIGINT);
        assertFalse(domain.includesNullableValue(Long.MIN_VALUE));
        assertTrue(domain.includesNullableValue(0L));
        assertFalse(domain.includesNullableValue(Long.MAX_VALUE));
        assertEquals(domain.complement(), Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 0L), Range.greaterThan(BIGINT, 0L)), true));
        assertEquals(domain.getSingleValue(), 0L);
        assertEquals(domain.getNullableSingleValue(), 0L);

        try {
            Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 1L, true, 2L, true)), false).getSingleValue();
            fail();
        }
        catch (IllegalStateException e) {
        }
    }

    @Test
    public void testEquatableSingleValue()
    {
        Domain domain = Domain.singleValue(ID, 0L);
        assertFalse(domain.isNone());
        assertFalse(domain.isAll());
        assertTrue(domain.isSingleValue());
        assertTrue(domain.isNullableSingleValue());
        assertFalse(domain.isOnlyNull());
        assertFalse(domain.isNullAllowed());
        assertEquals(domain.getValues(), ValueSet.of(ID, 0L));
        assertEquals(domain.getType(), ID);
        assertTrue(domain.includesNullableValue(0L));
        assertFalse(domain.includesNullableValue(null));
        assertEquals(domain.complement(), Domain.create(ValueSet.of(ID, 0L).complement(), true));
        assertEquals(domain.getSingleValue(), 0L);
        assertEquals(domain.getNullableSingleValue(), 0L);

        try {
            Domain.create(ValueSet.of(ID, 0L, 1L), false).getSingleValue();
            fail();
        }
        catch (IllegalStateException e) {
        }
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testUncomparableSingleValue()
    {
        Domain.singleValue(HYPER_LOG_LOG, Slices.EMPTY_SLICE);
    }

    @Test
    public void testOverlaps()
    {
        assertTrue(Domain.all(BIGINT).overlaps(Domain.all(BIGINT)));
        assertFalse(Domain.all(BIGINT).overlaps(Domain.none(BIGINT)));
        assertTrue(Domain.all(BIGINT).overlaps(Domain.notNull(BIGINT)));
        assertTrue(Domain.all(BIGINT).overlaps(Domain.onlyNull(BIGINT)));
        assertTrue(Domain.all(BIGINT).overlaps(Domain.singleValue(BIGINT, 0L)));

        assertFalse(Domain.none(BIGINT).overlaps(Domain.all(BIGINT)));
        assertFalse(Domain.none(BIGINT).overlaps(Domain.none(BIGINT)));
        assertFalse(Domain.none(BIGINT).overlaps(Domain.notNull(BIGINT)));
        assertFalse(Domain.none(BIGINT).overlaps(Domain.onlyNull(BIGINT)));
        assertFalse(Domain.none(BIGINT).overlaps(Domain.singleValue(BIGINT, 0L)));

        assertTrue(Domain.notNull(BIGINT).overlaps(Domain.all(BIGINT)));
        assertFalse(Domain.notNull(BIGINT).overlaps(Domain.none(BIGINT)));
        assertTrue(Domain.notNull(BIGINT).overlaps(Domain.notNull(BIGINT)));
        assertFalse(Domain.notNull(BIGINT).overlaps(Domain.onlyNull(BIGINT)));
        assertTrue(Domain.notNull(BIGINT).overlaps(Domain.singleValue(BIGINT, 0L)));

        assertTrue(Domain.onlyNull(BIGINT).overlaps(Domain.all(BIGINT)));
        assertFalse(Domain.onlyNull(BIGINT).overlaps(Domain.none(BIGINT)));
        assertFalse(Domain.onlyNull(BIGINT).overlaps(Domain.notNull(BIGINT)));
        assertTrue(Domain.onlyNull(BIGINT).overlaps(Domain.onlyNull(BIGINT)));
        assertFalse(Domain.onlyNull(BIGINT).overlaps(Domain.singleValue(BIGINT, 0L)));

        assertTrue(Domain.singleValue(BIGINT, 0L).overlaps(Domain.all(BIGINT)));
        assertFalse(Domain.singleValue(BIGINT, 0L).overlaps(Domain.none(BIGINT)));
        assertTrue(Domain.singleValue(BIGINT, 0L).overlaps(Domain.notNull(BIGINT)));
        assertFalse(Domain.singleValue(BIGINT, 0L).overlaps(Domain.onlyNull(BIGINT)));
        assertTrue(Domain.singleValue(BIGINT, 0L).overlaps(Domain.singleValue(BIGINT, 0L)));
    }

    @Test
    public void testContains()
    {
        assertTrue(Domain.all(BIGINT).contains(Domain.all(BIGINT)));
        assertTrue(Domain.all(BIGINT).contains(Domain.none(BIGINT)));
        assertTrue(Domain.all(BIGINT).contains(Domain.notNull(BIGINT)));
        assertTrue(Domain.all(BIGINT).contains(Domain.onlyNull(BIGINT)));
        assertTrue(Domain.all(BIGINT).contains(Domain.singleValue(BIGINT, 0L)));

        assertFalse(Domain.none(BIGINT).contains(Domain.all(BIGINT)));
        assertTrue(Domain.none(BIGINT).contains(Domain.none(BIGINT)));
        assertFalse(Domain.none(BIGINT).contains(Domain.notNull(BIGINT)));
        assertFalse(Domain.none(BIGINT).contains(Domain.onlyNull(BIGINT)));
        assertFalse(Domain.none(BIGINT).contains(Domain.singleValue(BIGINT, 0L)));

        assertFalse(Domain.notNull(BIGINT).contains(Domain.all(BIGINT)));
        assertTrue(Domain.notNull(BIGINT).contains(Domain.none(BIGINT)));
        assertTrue(Domain.notNull(BIGINT).contains(Domain.notNull(BIGINT)));
        assertFalse(Domain.notNull(BIGINT).contains(Domain.onlyNull(BIGINT)));
        assertTrue(Domain.notNull(BIGINT).contains(Domain.singleValue(BIGINT, 0L)));

        assertFalse(Domain.onlyNull(BIGINT).contains(Domain.all(BIGINT)));
        assertTrue(Domain.onlyNull(BIGINT).contains(Domain.none(BIGINT)));
        assertFalse(Domain.onlyNull(BIGINT).contains(Domain.notNull(BIGINT)));
        assertTrue(Domain.onlyNull(BIGINT).contains(Domain.onlyNull(BIGINT)));
        assertFalse(Domain.onlyNull(BIGINT).contains(Domain.singleValue(BIGINT, 0L)));

        assertFalse(Domain.singleValue(BIGINT, 0L).contains(Domain.all(BIGINT)));
        assertTrue(Domain.singleValue(BIGINT, 0L).contains(Domain.none(BIGINT)));
        assertFalse(Domain.singleValue(BIGINT, 0L).contains(Domain.notNull(BIGINT)));
        assertFalse(Domain.singleValue(BIGINT, 0L).contains(Domain.onlyNull(BIGINT)));
        assertTrue(Domain.singleValue(BIGINT, 0L).contains(Domain.singleValue(BIGINT, 0L)));
    }

    @Test
    public void testIntersect()
    {
        assertEquals(
                Domain.all(BIGINT).intersect(Domain.all(BIGINT)),
                Domain.all(BIGINT));

        assertEquals(
                Domain.none(BIGINT).intersect(Domain.none(BIGINT)),
                Domain.none(BIGINT));

        assertEquals(
                Domain.all(BIGINT).intersect(Domain.none(BIGINT)),
                Domain.none(BIGINT));

        assertEquals(
                Domain.notNull(BIGINT).intersect(Domain.onlyNull(BIGINT)),
                Domain.none(BIGINT));

        assertEquals(
                Domain.singleValue(BIGINT, 0L).intersect(Domain.all(BIGINT)),
                Domain.singleValue(BIGINT, 0L));

        assertEquals(
                Domain.singleValue(BIGINT, 0L).intersect(Domain.onlyNull(BIGINT)),
                Domain.none(BIGINT));

        assertEquals(
                Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L)), true).intersect(Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 2L)), true)),
                Domain.onlyNull(BIGINT));

        assertEquals(
                Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L)), true).intersect(Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L)), false)),
                Domain.singleValue(BIGINT, 1L));
    }

    @Test
    public void testUnion()
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
    {
        assertEquals(
                Domain.all(BIGINT).subtract(Domain.all(BIGINT)),
                Domain.none(BIGINT));
        assertEquals(
                Domain.all(BIGINT).subtract(Domain.none(BIGINT)),
                Domain.all(BIGINT));
        assertEquals(
                Domain.all(BIGINT).subtract(Domain.notNull(BIGINT)),
                Domain.onlyNull(BIGINT));
        assertEquals(
                Domain.all(BIGINT).subtract(Domain.onlyNull(BIGINT)),
                Domain.notNull(BIGINT));
        assertEquals(
                Domain.all(BIGINT).subtract(Domain.singleValue(BIGINT, 0L)),
                Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 0L), Range.greaterThan(BIGINT, 0L)), true));

        assertEquals(
                Domain.none(BIGINT).subtract(Domain.all(BIGINT)),
                Domain.none(BIGINT));
        assertEquals(
                Domain.none(BIGINT).subtract(Domain.none(BIGINT)),
                Domain.none(BIGINT));
        assertEquals(
                Domain.none(BIGINT).subtract(Domain.notNull(BIGINT)),
                Domain.none(BIGINT));
        assertEquals(
                Domain.none(BIGINT).subtract(Domain.onlyNull(BIGINT)),
                Domain.none(BIGINT));
        assertEquals(
                Domain.none(BIGINT).subtract(Domain.singleValue(BIGINT, 0L)),
                Domain.none(BIGINT));

        assertEquals(
                Domain.notNull(BIGINT).subtract(Domain.all(BIGINT)),
                Domain.none(BIGINT));
        assertEquals(
                Domain.notNull(BIGINT).subtract(Domain.none(BIGINT)),
                Domain.notNull(BIGINT));
        assertEquals(
                Domain.notNull(BIGINT).subtract(Domain.notNull(BIGINT)),
                Domain.none(BIGINT));
        assertEquals(
                Domain.notNull(BIGINT).subtract(Domain.onlyNull(BIGINT)),
                Domain.notNull(BIGINT));
        assertEquals(
                Domain.notNull(BIGINT).subtract(Domain.singleValue(BIGINT, 0L)),
                Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 0L), Range.greaterThan(BIGINT, 0L)), false));

        assertEquals(
                Domain.onlyNull(BIGINT).subtract(Domain.all(BIGINT)),
                Domain.none(BIGINT));
        assertEquals(
                Domain.onlyNull(BIGINT).subtract(Domain.none(BIGINT)),
                Domain.onlyNull(BIGINT));
        assertEquals(
                Domain.onlyNull(BIGINT).subtract(Domain.notNull(BIGINT)),
                Domain.onlyNull(BIGINT));
        assertEquals(
                Domain.onlyNull(BIGINT).subtract(Domain.onlyNull(BIGINT)),
                Domain.none(BIGINT));
        assertEquals(
                Domain.onlyNull(BIGINT).subtract(Domain.singleValue(BIGINT, 0L)),
                Domain.onlyNull(BIGINT));

        assertEquals(
                Domain.singleValue(BIGINT, 0L).subtract(Domain.all(BIGINT)),
                Domain.none(BIGINT));
        assertEquals(
                Domain.singleValue(BIGINT, 0L).subtract(Domain.none(BIGINT)),
                Domain.singleValue(BIGINT, 0L));
        assertEquals(
                Domain.singleValue(BIGINT, 0L).subtract(Domain.notNull(BIGINT)),
                Domain.none(BIGINT));
        assertEquals(
                Domain.singleValue(BIGINT, 0L).subtract(Domain.onlyNull(BIGINT)),
                Domain.singleValue(BIGINT, 0L));
        assertEquals(
                Domain.singleValue(BIGINT, 0L).subtract(Domain.singleValue(BIGINT, 0L)),
                Domain.none(BIGINT));

        assertEquals(
                Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L)), true).subtract(Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 2L)), true)),
                Domain.singleValue(BIGINT, 1L));

        assertEquals(
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
        assertEquals(domain, mapper.readValue(mapper.writeValueAsString(domain), Domain.class));

        domain = Domain.none(DOUBLE);
        assertEquals(domain, mapper.readValue(mapper.writeValueAsString(domain), Domain.class));

        domain = Domain.notNull(BOOLEAN);
        assertEquals(domain, mapper.readValue(mapper.writeValueAsString(domain), Domain.class));

        domain = Domain.notNull(HYPER_LOG_LOG);
        assertEquals(domain, mapper.readValue(mapper.writeValueAsString(domain), Domain.class));

        domain = Domain.onlyNull(VARCHAR);
        assertEquals(domain, mapper.readValue(mapper.writeValueAsString(domain), Domain.class));

        domain = Domain.onlyNull(HYPER_LOG_LOG);
        assertEquals(domain, mapper.readValue(mapper.writeValueAsString(domain), Domain.class));

        domain = Domain.singleValue(BIGINT, Long.MIN_VALUE);
        assertEquals(domain, mapper.readValue(mapper.writeValueAsString(domain), Domain.class));

        domain = Domain.singleValue(ID, Long.MIN_VALUE);
        assertEquals(domain, mapper.readValue(mapper.writeValueAsString(domain), Domain.class));

        domain = Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 0L), Range.equal(BIGINT, 1L), Range.range(BIGINT, 2L, true, 3L, true)), true);
        assertEquals(domain, mapper.readValue(mapper.writeValueAsString(domain), Domain.class));
    }

    private void assertUnion(Domain first, Domain second, Domain expected)
    {
        assertEquals(first.union(second), expected);
        assertEquals(Domain.union(ImmutableList.of(first, second)), expected);
    }
}
