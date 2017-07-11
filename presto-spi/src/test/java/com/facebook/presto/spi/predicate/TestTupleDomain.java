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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.TestingColumnHandle;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.TestingBlockEncodingSerde;
import com.facebook.presto.spi.block.TestingBlockJsonSerde;
import com.facebook.presto.spi.type.TestingTypeDeserializer;
import com.facebook.presto.spi.type.TestingTypeManager;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.ObjectMapperProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Map;

import static com.facebook.presto.spi.predicate.TupleDomain.columnWiseUnion;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestTupleDomain
{
    private static final ColumnHandle A = new TestingColumnHandle("a");
    private static final ColumnHandle B = new TestingColumnHandle("b");
    private static final ColumnHandle C = new TestingColumnHandle("c");
    private static final ColumnHandle D = new TestingColumnHandle("d");
    private static final ColumnHandle E = new TestingColumnHandle("e");
    private static final ColumnHandle F = new TestingColumnHandle("f");

    @Test
    public void testNone()
            throws Exception
    {
        assertTrue(TupleDomain.none().isNone());
        assertEquals(TupleDomain.<ColumnHandle>none(),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        A, Domain.none(BIGINT))));
        assertEquals(TupleDomain.<ColumnHandle>none(),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        A, Domain.all(BIGINT),
                        B, Domain.none(VARCHAR))));
    }

    @Test
    public void testAll()
            throws Exception
    {
        assertTrue(TupleDomain.all().isAll());
        assertEquals(TupleDomain.<ColumnHandle>all(),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        A, Domain.all(BIGINT))));
        assertEquals(TupleDomain.<ColumnHandle>all(),
                TupleDomain.withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of()));
    }

    @Test
    public void testIntersection()
            throws Exception
    {
        TupleDomain<ColumnHandle> tupleDomain1 = TupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, Domain>builder()
                        .put(A, Domain.all(VARCHAR))
                        .put(B, Domain.notNull(DOUBLE))
                        .put(C, Domain.singleValue(BIGINT, 1L))
                        .put(D, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(DOUBLE, 0.0)), true))
                        .build());

        TupleDomain<ColumnHandle> tupleDomain2 = TupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, Domain>builder()
                        .put(A, Domain.singleValue(VARCHAR, utf8Slice("value")))
                        .put(B, Domain.singleValue(DOUBLE, 0.0))
                        .put(C, Domain.singleValue(BIGINT, 1L))
                        .put(D, Domain.create(ValueSet.ofRanges(Range.lessThan(DOUBLE, 10.0)), false))
                        .build());

        TupleDomain<ColumnHandle> expectedTupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, Domain>builder()
                        .put(A, Domain.singleValue(VARCHAR, utf8Slice("value")))
                        .put(B, Domain.singleValue(DOUBLE, 0.0))
                        .put(C, Domain.singleValue(BIGINT, 1L))
                        .put(D, Domain.create(ValueSet.ofRanges(Range.range(DOUBLE, 0.0, true, 10.0, false)), false))
                        .build());

        assertEquals(tupleDomain1.intersect(tupleDomain2), expectedTupleDomain);
    }

    @Test
    public void testNoneIntersection()
            throws Exception
    {
        assertEquals(TupleDomain.none().intersect(TupleDomain.all()), TupleDomain.none());
        assertEquals(TupleDomain.all().intersect(TupleDomain.none()), TupleDomain.none());
        assertEquals(TupleDomain.none().intersect(TupleDomain.none()), TupleDomain.none());
        assertEquals(
                TupleDomain.withColumnDomains(ImmutableMap.of(A, Domain.onlyNull(BIGINT)))
                        .intersect(TupleDomain.withColumnDomains(ImmutableMap.of(A, Domain.notNull(BIGINT)))),
                TupleDomain.<ColumnHandle>none());
    }

    @Test
    public void testMismatchedColumnIntersection()
            throws Exception
    {
        TupleDomain<ColumnHandle> tupleDomain1 = TupleDomain.withColumnDomains(
                ImmutableMap.of(
                        A, Domain.all(DOUBLE),
                        B, Domain.singleValue(VARCHAR, utf8Slice("value"))));

        TupleDomain<ColumnHandle> tupleDomain2 = TupleDomain.withColumnDomains(
                ImmutableMap.of(
                        A, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(DOUBLE, 0.0)), true),
                        C, Domain.singleValue(BIGINT, 1L)));

        TupleDomain<ColumnHandle> expectedTupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                A, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(DOUBLE, 0.0)), true),
                B, Domain.singleValue(VARCHAR, utf8Slice("value")),
                C, Domain.singleValue(BIGINT, 1L)));

        assertEquals(tupleDomain1.intersect(tupleDomain2), expectedTupleDomain);
    }

    @Test
    public void testColumnWiseUnion()
            throws Exception
    {
        TupleDomain<ColumnHandle> tupleDomain1 = TupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, Domain>builder()
                        .put(A, Domain.all(VARCHAR))
                        .put(B, Domain.notNull(DOUBLE))
                        .put(C, Domain.onlyNull(BIGINT))
                        .put(D, Domain.singleValue(BIGINT, 1L))
                        .put(E, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(DOUBLE, 0.0)), true))
                        .build());

        TupleDomain<ColumnHandle> tupleDomain2 = TupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, Domain>builder()
                        .put(A, Domain.singleValue(VARCHAR, utf8Slice("value")))
                        .put(B, Domain.singleValue(DOUBLE, 0.0))
                        .put(C, Domain.notNull(BIGINT))
                        .put(D, Domain.singleValue(BIGINT, 1L))
                        .put(E, Domain.create(ValueSet.ofRanges(Range.lessThan(DOUBLE, 10.0)), false))
                        .build());

        TupleDomain<ColumnHandle> expectedTupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, Domain>builder()
                        .put(A, Domain.all(VARCHAR))
                        .put(B, Domain.notNull(DOUBLE))
                        .put(C, Domain.all(BIGINT))
                        .put(D, Domain.singleValue(BIGINT, 1L))
                        .put(E, Domain.all(DOUBLE))
                        .build());

        assertEquals(columnWiseUnion(tupleDomain1, tupleDomain2), expectedTupleDomain);
    }

    @Test
    public void testNoneColumnWiseUnion()
            throws Exception
    {
        assertEquals(columnWiseUnion(TupleDomain.none(), TupleDomain.all()), TupleDomain.all());
        assertEquals(columnWiseUnion(TupleDomain.all(), TupleDomain.none()), TupleDomain.all());
        assertEquals(columnWiseUnion(TupleDomain.none(), TupleDomain.none()), TupleDomain.none());
        assertEquals(
                columnWiseUnion(
                        TupleDomain.withColumnDomains(ImmutableMap.of(A, Domain.onlyNull(BIGINT))),
                        TupleDomain.withColumnDomains(ImmutableMap.of(A, Domain.notNull(BIGINT)))),
                TupleDomain.<ColumnHandle>all());
    }

    @Test
    public void testMismatchedColumnWiseUnion()
            throws Exception
    {
        TupleDomain<ColumnHandle> tupleDomain1 = TupleDomain.withColumnDomains(
                ImmutableMap.of(
                        A, Domain.all(DOUBLE),
                        B, Domain.singleValue(VARCHAR, utf8Slice("value"))));

        TupleDomain<ColumnHandle> tupleDomain2 = TupleDomain.withColumnDomains(
                ImmutableMap.of(
                        A, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(DOUBLE, 0.0)), true),
                        C, Domain.singleValue(BIGINT, 1L)));

        TupleDomain<ColumnHandle> expectedTupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(A, Domain.all(DOUBLE)));

        assertEquals(columnWiseUnion(tupleDomain1, tupleDomain2), expectedTupleDomain);
    }

    @Test
    public void testOverlaps()
            throws Exception
    {
        assertTrue(overlaps(
                ImmutableMap.of(),
                ImmutableMap.of()));

        assertTrue(overlaps(
                ImmutableMap.of(),
                ImmutableMap.of(A, Domain.singleValue(BIGINT, 0L))));

        assertFalse(overlaps(
                ImmutableMap.of(),
                ImmutableMap.of(A, Domain.none(BIGINT))));

        assertFalse(overlaps(
                ImmutableMap.of(A, Domain.none(BIGINT)),
                ImmutableMap.of(A, Domain.none(BIGINT))));

        assertTrue(overlaps(
                ImmutableMap.of(A, Domain.all(BIGINT)),
                ImmutableMap.of(A, Domain.all(BIGINT))));

        assertTrue(overlaps(
                ImmutableMap.of(A, Domain.singleValue(BIGINT, 1L)),
                ImmutableMap.of(B, Domain.singleValue(VARCHAR, utf8Slice("value")))));

        assertTrue(overlaps(
                ImmutableMap.of(A, Domain.singleValue(BIGINT, 1L)),
                ImmutableMap.of(A, Domain.all(BIGINT))));

        assertFalse(overlaps(
                ImmutableMap.of(A, Domain.singleValue(BIGINT, 1L)),
                ImmutableMap.of(A, Domain.singleValue(BIGINT, 2L))));

        assertFalse(overlaps(
                ImmutableMap.of(
                        A, Domain.singleValue(BIGINT, 1L),
                        B, Domain.singleValue(BIGINT, 1L)),
                ImmutableMap.of(
                        A, Domain.singleValue(BIGINT, 1L),
                        B, Domain.singleValue(BIGINT, 2L))));

        assertTrue(overlaps(
                ImmutableMap.of(
                        A, Domain.singleValue(BIGINT, 1L),
                        B, Domain.all(BIGINT)),
                ImmutableMap.of(
                        A, Domain.singleValue(BIGINT, 1L),
                        B, Domain.singleValue(BIGINT, 2L))));
    }

    @Test
    public void testContains()
            throws Exception
    {
        assertTrue(contains(
                ImmutableMap.of(),
                ImmutableMap.of()));

        assertTrue(contains(
                ImmutableMap.of(),
                ImmutableMap.of(A, Domain.none(BIGINT))));

        assertTrue(contains(
                ImmutableMap.of(),
                ImmutableMap.of(A, Domain.all(BIGINT))));

        assertTrue(contains(
                ImmutableMap.of(),
                ImmutableMap.of(A, Domain.singleValue(DOUBLE, 0.0))));

        assertFalse(contains(
                ImmutableMap.of(A, Domain.none(BIGINT)),
                ImmutableMap.of()));

        assertTrue(contains(
                ImmutableMap.of(A, Domain.none(BIGINT)),
                ImmutableMap.of(A, Domain.none(BIGINT))));

        assertFalse(contains(
                ImmutableMap.of(A, Domain.none(BIGINT)),
                ImmutableMap.of(A, Domain.all(BIGINT))));

        assertFalse(contains(
                ImmutableMap.of(A, Domain.none(BIGINT)),
                ImmutableMap.of(A, Domain.singleValue(BIGINT, 0L))));

        assertTrue(contains(
                ImmutableMap.of(A, Domain.all(BIGINT)),
                ImmutableMap.of()));

        assertTrue(contains(
                ImmutableMap.of(A, Domain.all(BIGINT)),
                ImmutableMap.of(A, Domain.none(BIGINT))));

        assertTrue(contains(
                ImmutableMap.of(A, Domain.all(BIGINT)),
                ImmutableMap.of(A, Domain.all(BIGINT))));

        assertTrue(contains(
                ImmutableMap.of(A, Domain.all(BIGINT)),
                ImmutableMap.of(A, Domain.singleValue(BIGINT, 0L))));

        assertFalse(contains(
                ImmutableMap.of(A, Domain.singleValue(BIGINT, 0L)),
                ImmutableMap.of()));

        assertTrue(contains(
                ImmutableMap.of(A, Domain.singleValue(BIGINT, 0L)),
                ImmutableMap.of(A, Domain.none(BIGINT))));

        assertFalse(contains(
                ImmutableMap.of(A, Domain.singleValue(BIGINT, 0L)),
                ImmutableMap.of(A, Domain.all(BIGINT))));

        assertTrue(contains(
                ImmutableMap.of(A, Domain.singleValue(BIGINT, 0L)),
                ImmutableMap.of(A, Domain.singleValue(BIGINT, 0L))));

        assertFalse(contains(
                ImmutableMap.of(A, Domain.singleValue(BIGINT, 0L)),
                ImmutableMap.of(B, Domain.singleValue(VARCHAR, utf8Slice("value")))));

        assertFalse(contains(
                ImmutableMap.of(
                        A, Domain.singleValue(BIGINT, 0L),
                        B, Domain.singleValue(VARCHAR, utf8Slice("value"))),
                ImmutableMap.of(B, Domain.singleValue(VARCHAR, utf8Slice("value")))));

        assertTrue(contains(
                ImmutableMap.of(
                        A, Domain.singleValue(BIGINT, 0L),
                        B, Domain.singleValue(VARCHAR, utf8Slice("value"))),
                ImmutableMap.of(B, Domain.none(VARCHAR))));

        assertTrue(contains(
                ImmutableMap.of(
                        A, Domain.singleValue(BIGINT, 0L),
                        B, Domain.singleValue(VARCHAR, utf8Slice("value"))),
                ImmutableMap.of(
                        A, Domain.singleValue(BIGINT, 1L),
                        B, Domain.none(VARCHAR))));

        assertTrue(contains(
                ImmutableMap.of(
                        B, Domain.singleValue(VARCHAR, utf8Slice("value"))),
                ImmutableMap.of(
                        A, Domain.singleValue(BIGINT, 0L),
                        B, Domain.singleValue(VARCHAR, utf8Slice("value")))));

        assertTrue(contains(
                ImmutableMap.of(
                        A, Domain.all(BIGINT),
                        B, Domain.singleValue(VARCHAR, utf8Slice("value"))),
                ImmutableMap.of(
                        A, Domain.singleValue(BIGINT, 0L),
                        B, Domain.singleValue(VARCHAR, utf8Slice("value")))));

        assertFalse(contains(
                ImmutableMap.of(
                        A, Domain.all(BIGINT),
                        B, Domain.singleValue(VARCHAR, utf8Slice("value"))),
                ImmutableMap.of(
                        A, Domain.singleValue(BIGINT, 0L),
                        B, Domain.singleValue(VARCHAR, utf8Slice("value2")))));

        assertTrue(contains(
                ImmutableMap.of(
                        A, Domain.all(BIGINT),
                        B, Domain.singleValue(VARCHAR, utf8Slice("value"))),
                ImmutableMap.of(
                        A, Domain.singleValue(BIGINT, 0L),
                        B, Domain.singleValue(VARCHAR, utf8Slice("value2")),
                        C, Domain.none(VARCHAR))));

        assertFalse(contains(
                ImmutableMap.of(
                        A, Domain.all(BIGINT),
                        B, Domain.singleValue(VARCHAR, utf8Slice("value")),
                        C, Domain.none(VARCHAR)),
                ImmutableMap.of(
                        A, Domain.singleValue(BIGINT, 0L),
                        B, Domain.singleValue(VARCHAR, utf8Slice("value2")))));

        assertTrue(contains(
                ImmutableMap.of(
                        A, Domain.all(BIGINT),
                        B, Domain.singleValue(VARCHAR, utf8Slice("value")),
                        C, Domain.none(VARCHAR)),
                ImmutableMap.of(
                        A, Domain.singleValue(BIGINT, 0L),
                        B, Domain.none(VARCHAR))));
    }

    @Test
    public void testEquals()
            throws Exception
    {
        assertTrue(equals(
                ImmutableMap.of(),
                ImmutableMap.of()));

        assertTrue(equals(
                ImmutableMap.of(),
                ImmutableMap.of(A, Domain.all(BIGINT))));

        assertFalse(equals(
                ImmutableMap.of(),
                ImmutableMap.of(A, Domain.none(BIGINT))));

        assertFalse(equals(
                ImmutableMap.of(),
                ImmutableMap.of(A, Domain.singleValue(BIGINT, 0L))));

        assertTrue(equals(
                ImmutableMap.of(A, Domain.all(BIGINT)),
                ImmutableMap.of(A, Domain.all(BIGINT))));

        assertFalse(equals(
                ImmutableMap.of(A, Domain.all(BIGINT)),
                ImmutableMap.of(A, Domain.none(BIGINT))));

        assertFalse(equals(
                ImmutableMap.of(A, Domain.all(BIGINT)),
                ImmutableMap.of(A, Domain.singleValue(BIGINT, 0L))));

        assertTrue(equals(
                ImmutableMap.of(A, Domain.none(BIGINT)),
                ImmutableMap.of(A, Domain.none(BIGINT))));

        assertFalse(equals(
                ImmutableMap.of(A, Domain.none(BIGINT)),
                ImmutableMap.of(A, Domain.singleValue(BIGINT, 0L))));

        assertTrue(equals(
                ImmutableMap.of(A, Domain.singleValue(BIGINT, 0L)),
                ImmutableMap.of(A, Domain.singleValue(BIGINT, 0L))));

        assertFalse(equals(
                ImmutableMap.of(A, Domain.singleValue(BIGINT, 0L)),
                ImmutableMap.of(B, Domain.singleValue(BIGINT, 0L))));

        assertFalse(equals(
                ImmutableMap.of(A, Domain.singleValue(BIGINT, 0L)),
                ImmutableMap.of(A, Domain.singleValue(BIGINT, 1L))));

        assertTrue(equals(
                ImmutableMap.of(A, Domain.all(BIGINT)),
                ImmutableMap.of(B, Domain.all(VARCHAR))));

        assertTrue(equals(
                ImmutableMap.of(A, Domain.none(BIGINT)),
                ImmutableMap.of(B, Domain.none(VARCHAR))));

        assertTrue(equals(
                ImmutableMap.of(A, Domain.none(BIGINT)),
                ImmutableMap.of(
                        A, Domain.singleValue(BIGINT, 0L),
                        B, Domain.none(VARCHAR))));

        assertFalse(equals(
                ImmutableMap.of(
                        A, Domain.singleValue(BIGINT, 1L)),
                ImmutableMap.of(
                        A, Domain.singleValue(BIGINT, 0L),
                        B, Domain.none(VARCHAR))));

        assertTrue(equals(
                ImmutableMap.of(
                        A, Domain.singleValue(BIGINT, 1L),
                        C, Domain.none(DOUBLE)),
                ImmutableMap.of(
                        A, Domain.singleValue(BIGINT, 0L),
                        B, Domain.none(VARCHAR))));

        assertTrue(equals(
                ImmutableMap.of(
                        A, Domain.singleValue(BIGINT, 0L),
                        B, Domain.all(DOUBLE)),
                ImmutableMap.of(
                        A, Domain.singleValue(BIGINT, 0L),
                        B, Domain.all(DOUBLE))));

        assertTrue(equals(
                ImmutableMap.of(
                        A, Domain.singleValue(BIGINT, 0L),
                        B, Domain.all(VARCHAR)),
                ImmutableMap.of(
                        A, Domain.singleValue(BIGINT, 0L),
                        C, Domain.all(DOUBLE))));

        assertFalse(equals(
                ImmutableMap.of(
                        A, Domain.singleValue(BIGINT, 0L),
                        B, Domain.all(VARCHAR)),
                ImmutableMap.of(
                        A, Domain.singleValue(BIGINT, 1L),
                        C, Domain.all(DOUBLE))));

        assertFalse(equals(
                ImmutableMap.of(
                        A, Domain.singleValue(BIGINT, 0L),
                        B, Domain.all(VARCHAR)),
                ImmutableMap.of(
                        A, Domain.singleValue(BIGINT, 0L),
                        C, Domain.singleValue(DOUBLE, 0.0))));
    }

    @Test
    public void testIsNone()
            throws Exception
    {
        assertFalse(TupleDomain.withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of()).isNone());
        assertFalse(TupleDomain.withColumnDomains(ImmutableMap.of(A, Domain.singleValue(BIGINT, 0L))).isNone());
        assertTrue(TupleDomain.withColumnDomains(ImmutableMap.of(A, Domain.none(BIGINT))).isNone());
        assertFalse(TupleDomain.withColumnDomains(ImmutableMap.of(A, Domain.all(BIGINT))).isNone());
        assertTrue(TupleDomain.withColumnDomains(ImmutableMap.of(A, Domain.all(BIGINT), B, Domain.none(BIGINT))).isNone());
    }

    @Test
    public void testIsAll()
            throws Exception
    {
        assertTrue(TupleDomain.withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of()).isAll());
        assertFalse(TupleDomain.withColumnDomains(ImmutableMap.of(A, Domain.singleValue(BIGINT, 0L))).isAll());
        assertTrue(TupleDomain.withColumnDomains(ImmutableMap.of(A, Domain.all(BIGINT))).isAll());
        assertFalse(TupleDomain.withColumnDomains(ImmutableMap.of(A, Domain.singleValue(BIGINT, 0L), B, Domain.all(BIGINT))).isAll());
    }

    @Test
    public void testExtractFixedValues()
            throws Exception
    {
        assertEquals(
                TupleDomain.extractFixedValues(TupleDomain.withColumnDomains(
                        ImmutableMap.<ColumnHandle, Domain>builder()
                                .put(A, Domain.all(DOUBLE))
                                .put(B, Domain.singleValue(VARCHAR, utf8Slice("value")))
                                .put(C, Domain.onlyNull(BIGINT))
                                .put(D, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L)), true))
                                .build())).get(),
                ImmutableMap.of(
                        B, NullableValue.of(VARCHAR, utf8Slice("value")),
                        C, NullableValue.asNull(BIGINT)));
    }

    @Test
    public void testExtractFixedValuesFromNone()
            throws Exception
    {
        assertFalse(TupleDomain.extractFixedValues(TupleDomain.none()).isPresent());
    }

    @Test
    public void testExtractFixedValuesFromAll()
            throws Exception
    {
        assertEquals(TupleDomain.extractFixedValues(TupleDomain.all()).get(), ImmutableMap.of());
    }

    @Test
    public void testSingleValuesMapToDomain()
            throws Exception
    {
        assertEquals(
                TupleDomain.fromFixedValues(
                        ImmutableMap.<ColumnHandle, NullableValue>builder()
                                .put(A, NullableValue.of(BIGINT, 1L))
                                .put(B, NullableValue.of(VARCHAR, utf8Slice("value")))
                                .put(C, NullableValue.of(DOUBLE, 0.01))
                                .put(D, NullableValue.asNull(BOOLEAN))
                                .build()),
                TupleDomain.withColumnDomains(ImmutableMap.<ColumnHandle, Domain>builder()
                        .put(A, Domain.singleValue(BIGINT, 1L))
                        .put(B, Domain.singleValue(VARCHAR, utf8Slice("value")))
                        .put(C, Domain.singleValue(DOUBLE, 0.01))
                        .put(D, Domain.onlyNull(BOOLEAN))
                        .build()));
    }

    @Test
    public void testEmptySingleValuesMapToDomain()
            throws Exception
    {
        assertEquals(TupleDomain.fromFixedValues(ImmutableMap.of()), TupleDomain.all());
    }

    @Test
    public void testJsonSerialization()
            throws Exception
    {
        TestingTypeManager typeManager = new TestingTypeManager();
        TestingBlockEncodingSerde blockEncodingSerde = new TestingBlockEncodingSerde(typeManager);

        ObjectMapper mapper = new ObjectMapperProvider().get()
                .registerModule(new SimpleModule()
                        .addDeserializer(ColumnHandle.class, new JsonDeserializer<ColumnHandle>()
                        {
                            @Override
                            public ColumnHandle deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
                                    throws IOException
                            {
                                return new ObjectMapperProvider().get().readValue(jsonParser, TestingColumnHandle.class);
                            }
                        })
                        .addDeserializer(Type.class, new TestingTypeDeserializer(typeManager))
                        .addSerializer(Block.class, new TestingBlockJsonSerde.Serializer(blockEncodingSerde))
                        .addDeserializer(Block.class, new TestingBlockJsonSerde.Deserializer(blockEncodingSerde)));

        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.all();
        assertEquals(tupleDomain, mapper.readValue(mapper.writeValueAsString(tupleDomain), new TypeReference<TupleDomain<ColumnHandle>>() {}));

        tupleDomain = TupleDomain.none();
        assertEquals(tupleDomain, mapper.readValue(mapper.writeValueAsString(tupleDomain), new TypeReference<TupleDomain<ColumnHandle>>() {}));

        tupleDomain = TupleDomain.fromFixedValues(ImmutableMap.of(A, NullableValue.of(BIGINT, 1L), B, NullableValue.asNull(VARCHAR)));
        assertEquals(tupleDomain, mapper.readValue(mapper.writeValueAsString(tupleDomain), new TypeReference<TupleDomain<ColumnHandle>>() {}));
    }

    @Test
    public void testTransform()
            throws Exception
    {
        Map<Integer, Domain> domains = ImmutableMap.<Integer, Domain>builder()
                .put(1, Domain.singleValue(BIGINT, 1L))
                .put(2, Domain.singleValue(BIGINT, 2L))
                .put(3, Domain.singleValue(BIGINT, 3L))
                .build();

        TupleDomain<Integer> domain = TupleDomain.withColumnDomains(domains);
        TupleDomain<String> transformed = domain.transform(Object::toString);

        Map<String, Domain> expected = ImmutableMap.<String, Domain>builder()
                .put("1", Domain.singleValue(BIGINT, 1L))
                .put("2", Domain.singleValue(BIGINT, 2L))
                .put("3", Domain.singleValue(BIGINT, 3L))
                .build();

        assertEquals(transformed.getDomains().get(), expected);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testTransformFailsWithNonUniqueMapping()
            throws Exception
    {
        Map<Integer, Domain> domains = ImmutableMap.<Integer, Domain>builder()
                .put(1, Domain.singleValue(BIGINT, 1L))
                .put(2, Domain.singleValue(BIGINT, 2L))
                .put(3, Domain.singleValue(BIGINT, 3L))
                .build();

        TupleDomain<Integer> domain = TupleDomain.withColumnDomains(domains);

        domain.transform(input -> "x");
    }

    private boolean overlaps(Map<ColumnHandle, Domain> domains1, Map<ColumnHandle, Domain> domains2)
    {
        TupleDomain<ColumnHandle> tupleDomain1 = TupleDomain.withColumnDomains(domains1);
        TupleDomain<ColumnHandle> tupleDOmain2 = TupleDomain.withColumnDomains(domains2);
        return tupleDomain1.overlaps(tupleDOmain2);
    }

    private boolean contains(Map<ColumnHandle, Domain> superSet, Map<ColumnHandle, Domain> subSet)
    {
        TupleDomain<ColumnHandle> superSetTupleDomain = TupleDomain.withColumnDomains(superSet);
        TupleDomain<ColumnHandle> subSetTupleDomain = TupleDomain.withColumnDomains(subSet);
        return superSetTupleDomain.contains(subSetTupleDomain);
    }

    private boolean equals(Map<ColumnHandle, Domain> domains1, Map<ColumnHandle, Domain> domains2)
    {
        TupleDomain<ColumnHandle> tupleDomain1 = TupleDomain.withColumnDomains(domains1);
        TupleDomain<ColumnHandle> tupleDOmain2 = TupleDomain.withColumnDomains(domains2);
        return tupleDomain1.equals(tupleDOmain2);
    }
}
