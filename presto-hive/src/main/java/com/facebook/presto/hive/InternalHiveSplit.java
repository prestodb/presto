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
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.fs.Path;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.slice.SizeOf.sizeOfObjectArray;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

@NotThreadSafe
public class InternalHiveSplit
{
    // Overhead of ImmutableList and ImmutableMap is not accounted because of its complexity.
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(InternalHiveSplit.class).instanceSize();

    private static final int HOST_ADDRESS_INSTANCE_SIZE = ClassLayout.parseClass(HostAddress.class).instanceSize() +
            ClassLayout.parseClass(String.class).instanceSize();

    private final byte[] relativeUri;
    private final long end;
    private final long fileSize;
    private final long fileModifiedTime;

    // encode the hive blocks as an array of longs and list of list of addresses to save memory
    //if all blockAddress lists are empty, store only the empty list
    private final long[] blockEndOffsets;
    private final List<List<HostAddress>> blockAddresses;

    // stored as ints rather than Optionals to save memory
    // -1 indicates an absent value
    private final int readBucketNumber;
    private final int tableBucketNumber;

    private final boolean splittable;
    private final NodeSelectionStrategy nodeSelectionStrategy;
    private final boolean s3SelectPushdownEnabled;
    private final HiveSplitPartitionInfo partitionInfo;
    private final Optional<byte[]> extraFileInfo;
    private final Optional<EncryptionInformation> encryptionInformation;
    private final Map<String, String> customSplitInfo;

    private long start;
    private int currentBlockIndex;

    public InternalHiveSplit(
            String relativeUri,
            long start,
            long end,
            long fileSize,
            long fileModifiedTime,
            List<InternalHiveBlock> blocks,
            OptionalInt readBucketNumber,
            OptionalInt tableBucketNumber,
            boolean splittable,
            NodeSelectionStrategy nodeSelectionStrategy,
            boolean s3SelectPushdownEnabled,
            HiveSplitPartitionInfo partitionInfo,
            Optional<byte[]> extraFileInfo,
            Optional<EncryptionInformation> encryptionInformation,
            Map<String, String> customSplitInfo)
    {
        checkArgument(start >= 0, "start must be positive");
        checkArgument(end >= 0, "end must be positive");
        checkArgument(fileSize >= 0, "fileSize must be positive");
        checkArgument(fileModifiedTime >= 0, "fileModifiedTime must be positive");
        requireNonNull(relativeUri, "relativeUri is null");
        requireNonNull(readBucketNumber, "readBucketNumber is null");
        requireNonNull(tableBucketNumber, "tableBucketNumber is null");
        requireNonNull(nodeSelectionStrategy, "nodeSelectionStrategy is null");
        requireNonNull(partitionInfo, "partitionInfo is null");
        requireNonNull(extraFileInfo, "extraFileInfo is null");
        requireNonNull(encryptionInformation, "encryptionInformation is null");

        this.relativeUri = relativeUri.getBytes(UTF_8);
        this.start = start;
        this.end = end;
        this.fileSize = fileSize;
        this.fileModifiedTime = fileModifiedTime;
        this.readBucketNumber = readBucketNumber.orElse(-1);
        this.tableBucketNumber = tableBucketNumber.orElse(-1);
        this.splittable = splittable;
        this.nodeSelectionStrategy = nodeSelectionStrategy;
        this.s3SelectPushdownEnabled = s3SelectPushdownEnabled;
        this.partitionInfo = partitionInfo;
        this.extraFileInfo = extraFileInfo;
        this.customSplitInfo = ImmutableMap
            .copyOf(requireNonNull(customSplitInfo, "customSplitInfo is null"));

        ImmutableList.Builder<List<HostAddress>> addressesBuilder = ImmutableList.builder();
        blockEndOffsets = new long[blocks.size()];
        boolean allAddressesEmpty = true;
        for (int i = 0; i < blocks.size(); i++) {
            InternalHiveBlock block = blocks.get(i);
            List<HostAddress> addresses = block.getAddresses();
            allAddressesEmpty = allAddressesEmpty && addresses.isEmpty();
            addressesBuilder.add(addresses);
            blockEndOffsets[i] = block.getEnd();
        }
        blockAddresses = allAddressesEmpty ? ImmutableList.of() : addressesBuilder.build();
        this.encryptionInformation = encryptionInformation;
    }

    public String getPath()
    {
        String relativePathString = new String(relativeUri, UTF_8);
        return new Path(partitionInfo.getPath().resolve(relativePathString)).toString();
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

    public long getFileModifiedTime()
    {
        return fileModifiedTime;
    }

    public boolean isS3SelectPushdownEnabled()
    {
        return s3SelectPushdownEnabled;
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
        return readBucketNumber >= 0 ? OptionalInt.of(readBucketNumber) : OptionalInt.empty();
    }

    public OptionalInt getTableBucketNumber()
    {
        return tableBucketNumber >= 0 ? OptionalInt.of(tableBucketNumber) : OptionalInt.empty();
    }

    public boolean isSplittable()
    {
        return splittable;
    }

    public NodeSelectionStrategy getNodeSelectionStrategy()
    {
        return nodeSelectionStrategy;
    }

    public TableToPartitionMapping getTableToPartitionMapping()
    {
        return partitionInfo.getTableToPartitionMapping();
    }

    public Optional<BucketConversion> getBucketConversion()
    {
        return partitionInfo.getBucketConversion();
    }

    public InternalHiveBlock currentBlock()
    {
        checkState(!isDone(), "All blocks have been consumed");
        List<HostAddress> addresses = blockAddresses.isEmpty() ? ImmutableList.of() : blockAddresses.get(currentBlockIndex);
        return new InternalHiveBlock(blockEndOffsets[currentBlockIndex], addresses);
    }

    public boolean isDone()
    {
        return currentBlockIndex == blockEndOffsets.length;
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

    public Optional<byte[]> getExtraFileInfo()
    {
        return extraFileInfo;
    }

    public Optional<EncryptionInformation> getEncryptionInformation()
    {
        return this.encryptionInformation;
    }

    public Map<String, String> getCustomSplitInfo()
    {
        return customSplitInfo;
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
        result += sizeOf(relativeUri);
        result += sizeOf(blockEndOffsets);
        if (!blockAddresses.isEmpty()) {
            result += sizeOfObjectArray(blockAddresses.size());
            for (List<HostAddress> addresses : blockAddresses) {
                result += sizeOfObjectArray(addresses.size());
                for (HostAddress address : addresses) {
                    result += HOST_ADDRESS_INSTANCE_SIZE + address.getHostText().length() * Character.BYTES;
                }
            }
        }
        if (extraFileInfo.isPresent()) {
            result += sizeOf(extraFileInfo.get());
        }
        return result;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("relativeUri", new String(relativeUri, UTF_8))
                .add("start", start)
                .add("end", end)
                .add("fileSize", fileSize)
                .toString();
    }

    public static class InternalHiveBlock
    {
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
    }
}
