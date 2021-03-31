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
package com.facebook.presto.orc;

import com.facebook.presto.orc.metadata.Stream;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.orc.StripeReader.getDiskRanges;
import static com.facebook.presto.orc.metadata.ColumnEncoding.DEFAULT_SEQUENCE_ID;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.DATA;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.ROW_INDEX;
import static org.testng.Assert.assertEquals;

public class TestStripeReader
{
    @Test
    public void testGetDiskRanges()
    {
        List<Stream> streams1 = ImmutableList.of(
                new Stream(3, ROW_INDEX, 5, true, DEFAULT_SEQUENCE_ID, Optional.of(15L)),
                new Stream(4, ROW_INDEX, 5, true),
                new Stream(3, DATA, 5, true, DEFAULT_SEQUENCE_ID, Optional.of(45L)),
                new Stream(4, DATA, 5, true));

        List<Stream> streams2 = ImmutableList.of(
                new Stream(0, ROW_INDEX, 5, true),
                new Stream(5, ROW_INDEX, 5, true, DEFAULT_SEQUENCE_ID, Optional.of(25L)),
                new Stream(0, DATA, 5, true, DEFAULT_SEQUENCE_ID, Optional.of(30L)),
                new Stream(5, DATA, 5, true, DEFAULT_SEQUENCE_ID, Optional.of(55L)));

        List<Stream> streams3 = ImmutableList.of(
                new Stream(1, ROW_INDEX, 5, true, DEFAULT_SEQUENCE_ID, Optional.of(5L)),
                new Stream(2, ROW_INDEX, 5, true),
                new Stream(1, DATA, 5, true, DEFAULT_SEQUENCE_ID, Optional.of(35L)),
                new Stream(2, DATA, 5, true));

        List<List<Stream>> streams = ImmutableList.of(streams1, streams2, streams3);
        Map<StreamId, DiskRange> actual = getDiskRanges(streams, (streamId) -> streamId.getColumn() != 1);

        Map<StreamId, DiskRange> expected = ImmutableMap.<StreamId, DiskRange>builder()
                .put(new StreamId(0, DEFAULT_SEQUENCE_ID, ROW_INDEX), new DiskRange(0, 5))
                .put(new StreamId(2, DEFAULT_SEQUENCE_ID, ROW_INDEX), new DiskRange(10, 5))
                .put(new StreamId(3, DEFAULT_SEQUENCE_ID, ROW_INDEX), new DiskRange(15, 5))
                .put(new StreamId(4, DEFAULT_SEQUENCE_ID, ROW_INDEX), new DiskRange(20, 5))
                .put(new StreamId(5, DEFAULT_SEQUENCE_ID, ROW_INDEX), new DiskRange(25, 5))
                .put(new StreamId(0, DEFAULT_SEQUENCE_ID, DATA), new DiskRange(30, 5))
                .put(new StreamId(2, DEFAULT_SEQUENCE_ID, DATA), new DiskRange(40, 5))
                .put(new StreamId(3, DEFAULT_SEQUENCE_ID, DATA), new DiskRange(45, 5))
                .put(new StreamId(4, DEFAULT_SEQUENCE_ID, DATA), new DiskRange(50, 5))
                .put(new StreamId(5, DEFAULT_SEQUENCE_ID, DATA), new DiskRange(55, 5))
                .build();
        assertEquals(actual, expected);
    }
}
