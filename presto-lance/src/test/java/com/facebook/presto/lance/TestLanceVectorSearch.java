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
package com.facebook.presto.lance;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.common.type.RealType;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

public class TestLanceVectorSearch
{
    private static final JsonCodec<LanceVectorSearch> CODEC = jsonCodec(LanceVectorSearch.class);

    @Test
    public void testJsonRoundTripWithGlobalTopK()
    {
        LanceVectorSearch search = new LanceVectorSearch(
                "vec", ImmutableList.of(1.0, 2.0), 5, LanceVectorSearch.METRIC_COSINE,
                Optional.of(10), Optional.empty(), Optional.empty(), true);
        LanceVectorSearch copy = CODEC.fromJson(CODEC.toJson(search));
        assertEquals(copy, search);
        assertTrue(copy.isRequiresGlobalTopK());
    }

    @Test
    public void testCompatConstructorDefaultsFlagToFalse()
    {
        LanceVectorSearch search = new LanceVectorSearch(
                "vec", ImmutableList.of(1.0), 5, LanceVectorSearch.METRIC_L2,
                Optional.empty(), Optional.empty(), Optional.empty());
        assertFalse(search.isRequiresGlobalTopK());
    }

    @Test
    public void testEqualsDistinguishesFlag()
    {
        LanceVectorSearch base = new LanceVectorSearch(
                "vec", ImmutableList.of(1.0), 5, LanceVectorSearch.METRIC_L2,
                Optional.empty(), Optional.empty(), Optional.empty(), false);
        LanceVectorSearch flagged = new LanceVectorSearch(
                "vec", ImmutableList.of(1.0), 5, LanceVectorSearch.METRIC_L2,
                Optional.empty(), Optional.empty(), Optional.empty(), true);
        assertNotEquals(base, flagged);
    }

    @Test
    public void testDistanceColumn()
    {
        LanceColumnHandle distance = LanceColumnHandle.distanceColumn();
        assertEquals(distance.getColumnName(), LanceVectorSearch.DISTANCE_COLUMN_NAME);
        assertEquals(distance.getColumnType(), RealType.REAL);
        assertTrue(distance.isDistance());
        assertTrue(distance.isSynthetic());
        assertFalse(distance.isNullable());
    }
}
