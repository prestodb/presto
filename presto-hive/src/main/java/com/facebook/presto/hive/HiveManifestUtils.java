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
package com.facebook.presto.hive;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;

import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.hive.HiveErrorCode.MALFORMED_HIVE_FILE_STATISTICS;
import static com.facebook.presto.hive.PartitionUpdate.FileWriteInfo;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.String.format;

public class HiveManifestUtils
{
    private static final int FILE_SIZE_CHANNEL = 0;
    private static final int ROW_COUNT_CHANNEL = 1;

    private HiveManifestUtils()
    {
    }

    public static Page createFileStatisticsPage(long fileSize, long rowCount)
    {
        // FileStatistics page layout:
        //
        // fileSize   rowCount
        //  X             X
        PageBuilder statsPageBuilder = new PageBuilder(ImmutableList.of(BIGINT, BIGINT));
        statsPageBuilder.declarePosition();
        BIGINT.writeLong(statsPageBuilder.getBlockBuilder(FILE_SIZE_CHANNEL), fileSize);
        BIGINT.writeLong(statsPageBuilder.getBlockBuilder(ROW_COUNT_CHANNEL), rowCount);

        return statsPageBuilder.build();
    }

    public static long getFileSize(Page statisticsPage, int position)
    {
        // FileStatistics page layout:
        //
        // fileSize   rowCount
        //  X             X

        if (position < 0 || position >= statisticsPage.getPositionCount()) {
            throw new PrestoException(MALFORMED_HIVE_FILE_STATISTICS, format("Invalid position: %d specified for FileStatistics page", position));
        }
        return BIGINT.getLong(statisticsPage.getBlock(FILE_SIZE_CHANNEL), position);
    }

    public static Optional<Page> createPartitionManifest(PartitionUpdate partitionUpdate)
    {
        // Manifest Page layout:
        //   fileName    fileSize
        //      X           X
        //      X           X
        //      X           X
        // ....
        PageBuilder manifestBuilder = new PageBuilder(ImmutableList.of(VARCHAR, BIGINT));
        BlockBuilder fileNameBuilder = manifestBuilder.getBlockBuilder(0);
        BlockBuilder fileSizeBuilder = manifestBuilder.getBlockBuilder(1);
        for (FileWriteInfo fileWriteInfo : partitionUpdate.getFileWriteInfos()) {
            if (!fileWriteInfo.getFileSize().isPresent()) {
                return Optional.empty();
            }
            manifestBuilder.declarePosition();
            VARCHAR.writeSlice(fileNameBuilder, utf8Slice(fileWriteInfo.getWriteFileName()));
            BIGINT.writeLong(fileSizeBuilder, fileWriteInfo.getFileSize().get());
        }
        return Optional.of(manifestBuilder.build());
    }
}
