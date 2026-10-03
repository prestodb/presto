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
package com.facebook.presto.execution.scheduler;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class TestRuntimeFilter
{
    private static final JsonCodec<RuntimeFilter> CODEC = jsonCodec(RuntimeFilter.class);

    @Test
    public void testDomainColumnWiseUnionMerge()
    {
        DomainRuntimeFilter a = new DomainRuntimeFilter(
                TupleDomain.withColumnDomains(ImmutableMap.of("k", Domain.singleValue(BIGINT, 1L))));
        DomainRuntimeFilter b = new DomainRuntimeFilter(
                TupleDomain.withColumnDomains(ImmutableMap.of("k", Domain.singleValue(BIGINT, 2L))));

        DomainRuntimeFilter merged = (DomainRuntimeFilter) a.mergeWith(b);

        Domain expected = Domain.multipleValues(BIGINT, ImmutableList.of(1L, 2L));
        assertEquals(merged.getDomain(), TupleDomain.withColumnDomains(ImmutableMap.of("k", expected)));
    }

    @Test
    public void testMergeAcrossRepresentationsRejected()
    {
        DomainRuntimeFilter a = new DomainRuntimeFilter(
                TupleDomain.withColumnDomains(ImmutableMap.of("k", Domain.singleValue(BIGINT, 1L))));
        assertThrows(IllegalArgumentException.class, () -> a.mergeWith(new OtherRuntimeFilter()));
    }

    private static class OtherRuntimeFilter
            implements RuntimeFilter
    {
        @Override
        public RuntimeFilter mergeWith(RuntimeFilter other)
        {
            return this;
        }

        @Override
        public TupleDomain<String> toTupleDomain(String column, com.facebook.presto.common.type.Type type)
        {
            return TupleDomain.all();
        }

        @Override
        public boolean isAll()
        {
            return true;
        }

        @Override
        public boolean isNone()
        {
            return false;
        }

        @Override
        public long estimatedRetainedSizeInBytes()
        {
            return 0;
        }
    }

    @Test
    public void testNoneReflectsEmptyBuild()
    {
        assertTrue(new DomainRuntimeFilter(TupleDomain.none()).isNone());
        assertFalse(new DomainRuntimeFilter(TupleDomain.all()).isNone());
    }

    @Test
    public void testDomainJsonRoundTrip()
    {
        // TupleDomain.none() has simple JSON form and doesn't require block encoding.
        DomainRuntimeFilter original = new DomainRuntimeFilter(TupleDomain.none());

        String json = CODEC.toJson(original);
        RuntimeFilter decoded = CODEC.fromJson(json);

        assertTrue(decoded instanceof DomainRuntimeFilter);
        assertTrue(((DomainRuntimeFilter) decoded).getDomain().isNone());
    }

    @Test
    public void testBareWireBackCompat()
    {
        // A bare TupleDomain serialization (no @kind) must decode to DomainRuntimeFilter.
        DomainRuntimeFilter original = new DomainRuntimeFilter(TupleDomain.none());
        String wire = CODEC.toJson(original);
        assertFalse(wire.contains("@kind"), "DomainRuntimeFilter must serialize without @kind: " + wire);

        RuntimeFilter decoded = CODEC.fromJson(wire);
        assertTrue(decoded instanceof DomainRuntimeFilter);
        assertTrue(((DomainRuntimeFilter) decoded).getDomain().isNone());
    }

    @Test
    public void testUnknownKindRejected()
    {
        assertThrows(Exception.class, () ->
                CODEC.fromJson("{\"@kind\":\"bloom\"}"));
    }
}
