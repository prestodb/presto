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
package com.facebook.presto.ducklake.reader.delete;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.FixedPageSource;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import org.roaringbitmap.longlong.LongBitmapDataProvider;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.ducklake.DuckLakeErrorCode.DUCKLAKE_BAD_DELETE_FILE;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.stream.IntStream.range;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

/**
 * Pure in-memory unit tests: builds small fake delete and data pages and exercises {@link
 * PositionDeleteFilter} and {@link DeleteFilterPageSource} without touching Parquet or the
 * DuckLake catalog.
 */
public class TestPositionDeleteFilter
{
    private static final String TEST_PATH = "/data/del/delete.parquet";

    @Test
    public void testTwoColumnDeleteFileDeletesEveryListedPosition()
    {
        ConnectorPageSource deleteSource = new FixedPageSource(ImmutableList.of(deletePage(new long[] {10, 11, 12}, null)));
        PositionDeleteFilter filter = PositionDeleteFilter.read(deleteSource, 0, TEST_PATH);

        assertFalse(filter.isEmpty());
        assertEquals(filter.getDeletedCount(), 3);
        for (long position : new long[] {10, 11, 12}) {
            assertTrue(filter.isDeleted(position), "expected position " + position + " to be deleted");
        }
        assertFalse(filter.isDeleted(9));
        assertFalse(filter.isDeleted(13));
    }

    @Test
    public void testThreeColumnDeleteFileAppliesSnapshotVisibility()
    {
        long[] positions = {0, 1, 2, 3};
        long[] snapshots = {24, 25, 24, 25};

        PositionDeleteFilter atFirstDelete = PositionDeleteFilter.read(
                new FixedPageSource(ImmutableList.of(deletePage(positions, snapshots))), 24, TEST_PATH);
        assertFalse(atFirstDelete.isEmpty());
        assertEquals(atFirstDelete.getDeletedCount(), 2);
        assertTrue(atFirstDelete.isDeleted(0));
        assertTrue(atFirstDelete.isDeleted(2));
        assertFalse(atFirstDelete.isDeleted(1));
        assertFalse(atFirstDelete.isDeleted(3));

        PositionDeleteFilter atSecondDelete = PositionDeleteFilter.read(
                new FixedPageSource(ImmutableList.of(deletePage(positions, snapshots))), 25, TEST_PATH);
        assertFalse(atSecondDelete.isEmpty());
        assertEquals(atSecondDelete.getDeletedCount(), 4);
        for (long position : positions) {
            assertTrue(atSecondDelete.isDeleted(position));
        }

        PositionDeleteFilter beforeAnyDelete = PositionDeleteFilter.read(
                new FixedPageSource(ImmutableList.of(deletePage(positions, snapshots))), 23, TEST_PATH);
        assertTrue(beforeAnyDelete.isEmpty());
        assertEquals(beforeAnyDelete.getDeletedCount(), 0);
    }

    @Test
    public void testAccumulateUnionsMultipleDeleteFiles()
    {
        LongBitmapDataProvider deletedPositions = new Roaring64Bitmap();
        PositionDeleteFilter.accumulate(
                new FixedPageSource(ImmutableList.of(deletePage(new long[] {1, 2}, null))), 0, "file-one.parquet", deletedPositions);
        PositionDeleteFilter.accumulate(
                new FixedPageSource(ImmutableList.of(deletePage(new long[] {7}, null))), 0, "file-two.parquet", deletedPositions);
        PositionDeleteFilter filter = PositionDeleteFilter.of(deletedPositions);

        assertFalse(filter.isEmpty());
        assertEquals(filter.getDeletedCount(), 3);
        assertTrue(filter.isDeleted(1));
        assertTrue(filter.isDeleted(2));
        assertTrue(filter.isDeleted(7));
        assertFalse(filter.isDeleted(3));
    }

    @Test
    public void testReadFailureMessageIncludesDeleteFilePath()
    {
        ConnectorPageSource failingSource = new FixedPageSource(ImmutableList.of(deletePage(new long[] {1}, null)))
        {
            @Override
            public Page getNextPage()
            {
                throw new RuntimeException("boom");
            }
        };

        try {
            PositionDeleteFilter.read(failingSource, 0, "/data/del/broken-delete.parquet");
            throw new AssertionError("expected PrestoException");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), DUCKLAKE_BAD_DELETE_FILE.toErrorCode());
            assertTrue(e.getMessage().contains("/data/del/broken-delete.parquet"), "expected message to contain the delete file path: " + e.getMessage());
            assertTrue(e.getMessage().contains("boom"), "expected message to contain the cause: " + e.getMessage());
        }
    }

    @Test
    public void testDeleteFilterPageSourceDropsDeletedPositions()
    {
        PositionDeleteFilter filter = PositionDeleteFilter.read(
                new FixedPageSource(ImmutableList.of(deletePage(new long[] {11, 13}, null))), 0, TEST_PATH);

        Page dataPage = dataPage(new int[] {100, 101, 102, 103}, new long[] {10, 11, 12, 13});
        ConnectorPageSource delegate = new FixedPageSource(ImmutableList.of(dataPage));
        DeleteFilterPageSource pageSource = new DeleteFilterPageSource(delegate, filter, 1, false);

        Page result = pageSource.getNextPage();
        assertEquals(result.getPositionCount(), 2);
        assertEquals(intValues(result.getBlock(0)), ImmutableList.of(100, 102));
        assertEquals(longValues(result.getBlock(1)), ImmutableList.of(10L, 12L));
    }

    @Test
    public void testDeleteFilterPageSourceReturnsSamePageInstanceWhenNothingDeleted()
    {
        PositionDeleteFilter filter = PositionDeleteFilter.read(
                new FixedPageSource(ImmutableList.of(deletePage(new long[] {999}, null))), 0, TEST_PATH);

        Page dataPage = dataPage(new int[] {100, 101}, new long[] {10, 11});
        ConnectorPageSource delegate = new FixedPageSource(ImmutableList.of(dataPage));
        DeleteFilterPageSource pageSource = new DeleteFilterPageSource(delegate, filter, 1, false);

        Page result = pageSource.getNextPage();
        assertSame(result, dataPage);
    }

    @Test
    public void testDeleteFilterPageSourceStripsAppendedPositionChannel()
    {
        PositionDeleteFilter filter = PositionDeleteFilter.read(
                new FixedPageSource(ImmutableList.of(deletePage(new long[] {11}, null))), 0, TEST_PATH);

        // The query did not ask for $row_position: it was appended as the last channel purely so
        // this class could filter, and must not appear in the output.
        Page dataPage = dataPage(new int[] {100, 101, 102}, new long[] {10, 11, 12});
        ConnectorPageSource delegate = new FixedPageSource(ImmutableList.of(dataPage));
        DeleteFilterPageSource pageSource = new DeleteFilterPageSource(delegate, filter, 1, true);

        Page result = pageSource.getNextPage();
        assertEquals(result.getChannelCount(), 1);
        assertEquals(intValues(result.getBlock(0)), ImmutableList.of(100, 102));
    }

    private static Page deletePage(long[] positions, long[] snapshots)
    {
        BlockBuilder pathBuilder = VARCHAR.createBlockBuilder(null, positions.length);
        BlockBuilder posBuilder = BIGINT.createBlockBuilder(null, positions.length);
        for (long position : positions) {
            VARCHAR.writeString(pathBuilder, "ignored");
            BIGINT.writeLong(posBuilder, position);
        }
        if (snapshots == null) {
            return new Page(positions.length, pathBuilder.build(), posBuilder.build());
        }
        BlockBuilder snapshotBuilder = BIGINT.createBlockBuilder(null, positions.length);
        for (long snapshot : snapshots) {
            BIGINT.writeLong(snapshotBuilder, snapshot);
        }
        return new Page(positions.length, pathBuilder.build(), posBuilder.build(), snapshotBuilder.build());
    }

    private static Page dataPage(int[] ids, long[] positions)
    {
        BlockBuilder idBuilder = INTEGER.createBlockBuilder(null, ids.length);
        BlockBuilder positionBuilder = BIGINT.createBlockBuilder(null, positions.length);
        for (int i = 0; i < ids.length; i++) {
            INTEGER.writeLong(idBuilder, ids[i]);
            BIGINT.writeLong(positionBuilder, positions[i]);
        }
        return new Page(ids.length, idBuilder.build(), positionBuilder.build());
    }

    private static List<Integer> intValues(Block block)
    {
        return range(0, block.getPositionCount())
                .mapToObj(position -> (int) INTEGER.getLong(block, position))
                .collect(toImmutableList());
    }

    private static List<Long> longValues(Block block)
    {
        return range(0, block.getPositionCount())
                .mapToObj(position -> BIGINT.getLong(block, position))
                .collect(toImmutableList());
    }
}
