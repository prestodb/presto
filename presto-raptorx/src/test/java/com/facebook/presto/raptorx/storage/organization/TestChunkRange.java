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
package com.facebook.presto.raptorx.storage.organization;

import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestChunkRange
{
    @Test
    public void testEnclosesIsSymmetric()
    {
        List<Type> types = ImmutableList.of(BIGINT, VARCHAR, BOOLEAN, TIMESTAMP);
        ChunkRange range = ChunkRange.of(new Tuple(types, 2L, "aaa", true, 1L), new Tuple(types, 5L, "ccc", false, 2L));
        assertTrue(range.encloses(range));
    }

    @Test
    public void testEnclosingRange()
    {
        List<Type> types1 = ImmutableList.of(BIGINT);
        ChunkRange range1 = ChunkRange.of(new Tuple(types1, 2L), new Tuple(types1, 5L));

        ChunkRange enclosesRange1 = ChunkRange.of(new Tuple(types1, 1L), new Tuple(types1, 10L));
        ChunkRange notEnclosesRange1 = ChunkRange.of(new Tuple(types1, 1L), new Tuple(types1, 4L));

        assertTrue(enclosesRange1.encloses(range1));
        assertFalse(notEnclosesRange1.encloses(range1));

        List<Type> types2 = ImmutableList.of(BIGINT, VARCHAR);
        ChunkRange range2 = ChunkRange.of(new Tuple(types2, 2L, "aaa"), new Tuple(types2, 5L, "ccc"));
        ChunkRange enclosesRange2 = ChunkRange.of(new Tuple(types2, 1L, "ccc"), new Tuple(types2, 10L, "ccc"));
        ChunkRange notEnclosesRange2 = ChunkRange.of(new Tuple(types2, 2L, "aaa"), new Tuple(types2, 5L, "bbb"));

        assertTrue(range2.encloses(range2));
        assertTrue(enclosesRange2.encloses(range2));
        assertFalse(notEnclosesRange2.encloses(range2));
    }

    @Test
    public void testOverlapsIsSymmetric()
    {
        List<Type> types = ImmutableList.of(BIGINT, VARCHAR, BOOLEAN, TIMESTAMP);
        ChunkRange range = ChunkRange.of(new Tuple(types, 2L, "aaa", true, 1L), new Tuple(types, 5L, "ccc", false, 2L));
        assertTrue(range.overlaps(range));
    }

    @Test
    public void testOverlappingRange()
    {
        List<Type> types1 = ImmutableList.of(BIGINT);
        ChunkRange range1 = ChunkRange.of(new Tuple(types1, 2L), new Tuple(types1, 5L));

        ChunkRange enclosesRange1 = ChunkRange.of(new Tuple(types1, 1L), new Tuple(types1, 10L));
        ChunkRange overlapsRange1 = ChunkRange.of(new Tuple(types1, 1L), new Tuple(types1, 4L));
        ChunkRange notOverlapsRange1 = ChunkRange.of(new Tuple(types1, 6L), new Tuple(types1, 8L));

        assertTrue(enclosesRange1.overlaps(range1));
        assertTrue(overlapsRange1.overlaps(range1));
        assertFalse(notOverlapsRange1.overlaps(range1));

        List<Type> types2 = ImmutableList.of(BIGINT, VARCHAR);
        ChunkRange range2 = ChunkRange.of(new Tuple(types2, 2L, "aaa"), new Tuple(types2, 5L, "ccc"));
        ChunkRange enclosesRange2 = ChunkRange.of(new Tuple(types2, 1L, "ccc"), new Tuple(types2, 10L, "ccc"));
        ChunkRange overlapsRange2 = ChunkRange.of(new Tuple(types2, 2L, "aaa"), new Tuple(types2, 5L, "bbb"));
        ChunkRange notOverlapsRange2 = ChunkRange.of(new Tuple(types2, 6L, "aaa"), new Tuple(types2, 8L, "bbb"));

        assertTrue(enclosesRange2.encloses(range2));
        assertTrue(overlapsRange2.overlaps(range2));
        assertFalse(notOverlapsRange2.overlaps(range2));
    }

    @Test
    public void testAdjacentRange()
    {
        List<Type> types1 = ImmutableList.of(BIGINT);
        ChunkRange range1 = ChunkRange.of(new Tuple(types1, 2L), new Tuple(types1, 5L));
        ChunkRange adjacentRange1 = ChunkRange.of(new Tuple(types1, 5L), new Tuple(types1, 10L));

        assertFalse(range1.adjacent(range1));

        assertTrue(adjacentRange1.adjacent(range1));
        assertTrue(range1.adjacent(adjacentRange1));

        List<Type> types2 = ImmutableList.of(BIGINT, VARCHAR);
        ChunkRange range2 = ChunkRange.of(new Tuple(types2, 2L, "aaa"), new Tuple(types2, 5L, "ccc"));
        ChunkRange adjacentRange2 = ChunkRange.of(new Tuple(types2, 5L, "ccc"), new Tuple(types2, 10L, "ccc"));
        ChunkRange subsetAdjacentRange2 = ChunkRange.of(new Tuple(types2, 5L, "ddd"), new Tuple(types2, 10L, "ccc"));
        ChunkRange overlapsRange2 = ChunkRange.of(new Tuple(types2, 3L, "aaa"), new Tuple(types2, 10L, "ccc"));
        ChunkRange notAdjacentRange2 = ChunkRange.of(new Tuple(types2, 6L, "ccc"), new Tuple(types2, 10L, "ccc"));

        assertTrue(adjacentRange2.adjacent(range2));
        assertTrue(subsetAdjacentRange2.adjacent(range2));
        assertFalse(overlapsRange2.adjacent(range2));
        assertFalse(notAdjacentRange2.adjacent(range2));
    }
}
