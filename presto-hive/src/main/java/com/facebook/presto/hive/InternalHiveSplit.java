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

import com.facebook.presto.hive.HiveSplit.BucketConversion;
import com.facebook.presto.spi.HostAddress;
import com.google.common.collect.ImmutableList;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.sizeOfObjectArray;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

@NotThreadSafe
public class InternalHiveSplit
{
    // Overhead of ImmutableList and ImmutableMap is not accounted because of its complexity.
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(InternalHiveSplit.class).instanceSize() +
            ClassLayout.parseClass(String.class).instanceSize() +
            ClassLayout.parseClass(String.class).instanceSize() +
            ClassLayout.parseClass(OptionalInt.class).instanceSize();

    private final byte[] path;
    private final long end;
    private final long fileSize;
    private final List<InternalHiveBlock> blocks;
    private final OptionalInt readBucketNumber;
    private final OptionalInt tableBucketNumber;
    private final boolean splittable;
    private final boolean forceLocalScheduling;
    private final boolean s3SelectPushdownEnabled;
    private final HiveSplitPartitionInfo partitionInfo;

    private long start;
    private int currentBlockIndex;

    public InternalHiveSplit(
            String path,
            long start,
            long end,
            long fileSize,
            List<InternalHiveBlock> blocks,
            OptionalInt readBucketNumber,
            OptionalInt tableBucketNumber,
            boolean splittable,
            boolean forceLocalScheduling,
            boolean s3SelectPushdownEnabled,
            HiveSplitPartitionInfo partitionInfo)
    {
        checkArgument(start >= 0, "start must be positive");
        checkArgument(end >= 0, "end must be positive");
        checkArgument(fileSize >= 0, "fileSize must be positive");
        requireNonNull(path, "path is null");
        requireNonNull(blocks, "blocks is null");
        requireNonNull(readBucketNumber, "readBucketNumber is null");
        requireNonNull(tableBucketNumber, "tableBucketNumber is null");
        requireNonNull(partitionInfo, "partitionInfo is null");

        this.path = path.getBytes(UTF_8);
        this.start = start;
        this.end = end;
        this.fileSize = fileSize;
        this.blocks = ImmutableList.copyOf(blocks);
        this.readBucketNumber = readBucketNumber;
        this.tableBucketNumber = tableBucketNumber;
        this.splittable = splittable;
        this.forceLocalScheduling = forceLocalScheduling;
        this.s3SelectPushdownEnabled = s3SelectPushdownEnabled;
        this.partitionInfo = partitionInfo;
    }

    public String getPath()
    {
        return new String(path, UTF_8);
    }

    public long getStart()
    {
        return start;
    }

    public long getEnd()
    {
        return end;
    }

    public long getFileSize()
    {
        return fileSize;
    }

    public boolean isS3SelectPushdownEnabled()
    {
        return s3SelectPushdownEnabled;
    }

    public Properties getSchema()
    {
        return partitionInfo.getSchema();
    }

    public List<HivePartitionKey> getPartitionKeys()
    {
        return partitionInfo.getPartitionKeys();
    }

    public String getPartitionName()
    {
        return partitionInfo.getPartitionName();
    }

    public OptionalInt getReadBucketNumber()
    {
        return readBucketNumber;
    }

    public OptionalInt getTableBucketNumber()
    {
        return tableBucketNumber;
    }

    public boolean isSplittable()
    {
        return splittable;
    }

    public boolean isForceLocalScheduling()
    {
        return forceLocalScheduling;
    }

    public Map<Integer, HiveTypeName> getColumnCoercions()
    {
        return partitionInfo.getColumnCoercions();
    }

    public Optional<BucketConversion> getBucketConversion()
    {
        return partitionInfo.getBucketConversion();
    }

    public InternalHiveBlock currentBlock()
    {
        checkState(!isDone(), "All blocks have been consumed");
        return blocks.get(currentBlockIndex);
    }

    public boolean isDone()
    {
        return currentBlockIndex == blocks.size();
    }

    public void increaseStart(long value)
    {
        start += value;
        if (start == currentBlock().getEnd()) {
            currentBlockIndex++;
        }
    }

    public HiveSplitPartitionInfo getPartitionInfo()
    {
        return partitionInfo;
    }

    public void reset()
    {
        currentBlockIndex = 0;
        start = 0;
    }

    /**
     * Estimate the size of this InternalHiveSplit. Note that
     * PartitionInfo is a shared object, so its memory usage is
     * tracked separately in HiveSplitSource.
     */
    public int getEstimatedSizeInBytes()
    {
        int result = INSTANCE_SIZE;
        result += path.length;
        result += sizeOfObjectArray(blocks.size());
        for (InternalHiveBlock block : blocks) {
            result += block.getEstimatedSizeInBytes();
        }
        return result;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("path", new String(relativeUri, UTF_8))
                .add("start", start)
                .add("end", end)
                .add("fileSize", fileSize)
                .toString();
    }

    public static class InternalHiveBlock
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(InternalHiveBlock.class).instanceSize();
        private static final int HOST_ADDRESS_INSTANCE_SIZE = ClassLayout.parseClass(HostAddress.class).instanceSize() +
                ClassLayout.parseClass(String.class).instanceSize();

        private final long end;
        private final List<HostAddress> addresses;

        public InternalHiveBlock(long end, List<HostAddress> addresses)
        {
            checkArgument(end >= 0, "block end must be >= 0");
            this.end = end;
            this.addresses = ImmutableList.copyOf(addresses);
        }

        public long getEnd()
        {
            return end;
        }

        public List<HostAddress> getAddresses()
        {
            return addresses;
        }

        public int getEstimatedSizeInBytes()
        {
            int result = INSTANCE_SIZE;
            result += sizeOfObjectArray(addresses.size());
            for (HostAddress address : addresses) {
                result += HOST_ADDRESS_INSTANCE_SIZE + address.getHostText().length() * Character.BYTES;
            }
            return result;
        }
    }
}
