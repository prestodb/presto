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
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.PrestoException;
import org.roaringbitmap.longlong.LongBitmapDataProvider;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.IOException;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.ducklake.DuckLakeErrorCode.DUCKLAKE_BAD_DELETE_FILE;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * The row positions one or more DuckLake positional delete files mark as deleted, restricted to
 * the ones visible at a given snapshot. Modeled on Iceberg's {@code PositionDeleteFilter}, but a
 * DuckLake delete file needs no path comparison: the catalog attaches a split's delete file(s)
 * through {@code ducklake_delete_file.data_file_id}, so every position any of them lists belongs
 * to the split's own data file (see {@link com.facebook.presto.ducklake.split.DeleteFile}).
 * Instead, a DuckLake delete file can carry a visibility check Iceberg has no equivalent for: when
 * its optional third column ({@code _ducklake_internal_snapshot_id}) is present, a listed position
 * is only deleted as of a query snapshot at or after that column's value for the row (mirroring the
 * extension's {@code DuckLakeDeleteData::Filter}); a row with no third column at all (an older,
 * two-column delete file) is unconditionally deleted.
 *
 * <p>{@code DuckLakeSplit.getDeletes()} is a list (kept Iceberg-shaped for a later native
 * translation), so a caller with more than one delete file for a split must union them into a
 * single shared bitmap via repeated {@link #accumulate} calls before building the filter with
 * {@link #of}, rather than calling {@link #read} (which only ever sees one file) more than once.
 */
public final class PositionDeleteFilter
{
    private final LongBitmapDataProvider deletedPositions;

    private PositionDeleteFilter(LongBitmapDataProvider deletedPositions)
    {
        this.deletedPositions = requireNonNull(deletedPositions, "deletedPositions is null");
    }

    /**
     * Reads one delete file's positions, deleted as of {@code snapshotId}, into a fresh filter.
     * Equivalent to calling {@link #accumulate} once into a new bitmap and wrapping it with {@link
     * #of}; use those two directly when a split has more than one delete file to union.
     */
    public static PositionDeleteFilter read(ConnectorPageSource deleteSource, long snapshotId, String path)
    {
        LongBitmapDataProvider deletedPositions = new Roaring64Bitmap();
        accumulate(deleteSource, snapshotId, path, deletedPositions);
        return new PositionDeleteFilter(deletedPositions);
    }

    /**
     * Reads every page of {@code deleteSource} (closing it when done), adding the positions
     * deleted as of {@code snapshotId} into the caller-supplied {@code deletedPositions} bitmap,
     * so several delete files can be unioned into one filter. {@code deleteSource} must produce
     * pages of exactly the shape {@code (file_path VARCHAR, pos BIGINT, snapshot BIGINT)} or
     * {@code (file_path VARCHAR, pos BIGINT)}; channel 0 is ignored (see the class javadoc for why
     * no path comparison is needed). {@code path} is used only to identify the file in an error
     * message if reading fails.
     */
    public static void accumulate(ConnectorPageSource deleteSource, long snapshotId, String path, LongBitmapDataProvider deletedPositions)
    {
        requireNonNull(deleteSource, "deleteSource is null");
        requireNonNull(deletedPositions, "deletedPositions is null");
        try (ConnectorPageSource source = deleteSource) {
            while (!source.isFinished()) {
                Page page = source.getNextPage();
                if (page == null) {
                    continue;
                }
                accumulatePage(page, snapshotId, deletedPositions);
            }
        }
        catch (IOException | RuntimeException e) {
            throw new PrestoException(DUCKLAKE_BAD_DELETE_FILE, format("Failed to read DuckLake delete file %s: %s", path, e.getMessage()), e);
        }
    }

    /** Wraps an already-populated bitmap (typically built by one or more {@link #accumulate} calls). */
    public static PositionDeleteFilter of(LongBitmapDataProvider deletedPositions)
    {
        return new PositionDeleteFilter(deletedPositions);
    }

    private static void accumulatePage(Page page, long snapshotId, LongBitmapDataProvider deletedPositions)
    {
        Block positionBlock = page.getBlock(1);
        Block snapshotBlock = page.getChannelCount() > 2 ? page.getBlock(2) : null;
        for (int position = 0; position < page.getPositionCount(); position++) {
            checkArgument(!positionBlock.isNull(position), "pos is null in DuckLake delete file");
            if (snapshotBlock == null || snapshotBlock.isNull(position) || BIGINT.getLong(snapshotBlock, position) <= snapshotId) {
                deletedPositions.addLong(BIGINT.getLong(positionBlock, position));
            }
        }
    }

    public boolean isDeleted(long position)
    {
        return deletedPositions.contains(position);
    }

    public boolean isEmpty()
    {
        return deletedPositions.isEmpty();
    }

    public long getDeletedCount()
    {
        return deletedPositions.getLongCardinality();
    }
}
