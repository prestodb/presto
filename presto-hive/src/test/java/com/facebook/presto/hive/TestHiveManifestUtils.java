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
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.StandardTypes.BOOLEAN;
import static com.facebook.presto.common.type.StandardTypes.DOUBLE;
import static com.facebook.presto.common.type.StandardTypes.VARCHAR;
import static com.facebook.presto.hive.HiveManifestUtils.createFileStatisticsPage;
import static com.facebook.presto.hive.HiveManifestUtils.createPartitionManifest;
import static com.facebook.presto.hive.HiveManifestUtils.getFileSize;
import static com.facebook.presto.hive.PartitionUpdate.FileWriteInfo;
import static com.facebook.presto.hive.PartitionUpdate.UpdateMode.NEW;
import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestHiveManifestUtils
{
    private static final long FILE_SIZE = 1024;
    private static final long ROW_COUNT = 100;

    @Test
    public void testCreateFileStatisticsPage()
    {
        Page statisticsPage = createFileStatisticsPage(FILE_SIZE, ROW_COUNT);

        assertEquals(statisticsPage.getPositionCount(), 1);
        assertEquals(statisticsPage.getChannelCount(), 2);
    }

    @Test
    public void testGetFileSize()
    {
        Page statisticsPage = createTestStatisticsPageWithOneRow(ImmutableList.of(BIGINT, BIGINT), ImmutableList.of(FILE_SIZE, ROW_COUNT));
        assertEquals(getFileSize(statisticsPage, 0), FILE_SIZE);
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Invalid position: 2 specified for FileStatistics page")
    public void testGetFileSizeOfInvalidStatisticsPage()
    {
        Page statisticsPage = createTestStatisticsPageWithOneRow(ImmutableList.of(BIGINT, BIGINT), ImmutableList.of(FILE_SIZE, ROW_COUNT));
        getFileSize(statisticsPage, 2);
    }

    @Test
    public void testCreatePartitionManifest()
    {
        PartitionUpdate partitionUpdate = new PartitionUpdate("testPartition", NEW, "/testDir", "/testDir", ImmutableList.of(new FileWriteInfo("testFileName", "testFileName", Optional.of(FILE_SIZE))), 100, 1024, 1024);
        Optional<Page> manifestPage = createPartitionManifest(partitionUpdate);
        assertTrue(manifestPage.isPresent());
        assertEquals(manifestPage.get().getChannelCount(), 2);
        assertEquals(manifestPage.get().getPositionCount(), 1);
    }

    private Page createTestStatisticsPageWithOneRow(List<Type> types, List<Object> values)
    {
        assertEquals(types.size(), values.size());
        PageBuilder pageBuilder = new PageBuilder(ImmutableList.copyOf(types));
        pageBuilder.declarePosition();
        for (int i = 0; i < types.size(); i++) {
            Type type = types.get(i);
            BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(i);
            Object value = values.get(i);
            switch (type.getTypeSignature().getBase()) {
                case BOOLEAN:
                    type.writeBoolean(blockBuilder, (Boolean) value);
                    break;
                case StandardTypes.BIGINT:
                    type.writeLong(blockBuilder, (Long) value);
                    break;
                case DOUBLE:
                    type.writeDouble(blockBuilder, (Double) value);
                    break;
                case VARCHAR:
                    type.writeSlice(blockBuilder, utf8Slice((String) value));
                    break;
            }
        }
        return pageBuilder.build();
    }
}
