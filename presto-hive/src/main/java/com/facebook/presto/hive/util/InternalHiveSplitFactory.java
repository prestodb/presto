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
package com.facebook.presto.hive.util;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.hive.EncryptionInformation;
import com.facebook.presto.hive.HiveFileInfo;
import com.facebook.presto.hive.HiveSplitPartitionInfo;
import com.facebook.presto.hive.InternalHiveSplit;
import com.facebook.presto.hive.InternalHiveSplit.InternalHiveBlock;
import com.facebook.presto.hive.S3SelectPushdown;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static com.facebook.presto.hive.HiveUtil.isSplittable;
import static com.facebook.presto.hive.util.CustomSplitConversionUtils.extractCustomSplitInfo;
import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.HARD_AFFINITY;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static java.util.Objects.requireNonNull;

public class InternalHiveSplitFactory
{
    private final FileSystem fileSystem;
    private final InputFormat<?, ?> inputFormat;
    private final Optional<Domain> pathDomain;
    private final NodeSelectionStrategy nodeSelectionStrategy;
    private final boolean s3SelectPushdownEnabled;
    private final HiveSplitPartitionInfo partitionInfo;
    private final boolean schedulerUsesHostAddresses;
    private final Optional<EncryptionInformation> encryptionInformation;
    private final long minimumTargetSplitSizeInBytes;

    public InternalHiveSplitFactory(
            FileSystem fileSystem,
            InputFormat<?, ?> inputFormat,
            Optional<Domain> pathDomain,
            NodeSelectionStrategy nodeSelectionStrategy,
            DataSize minimumTargetSplitSize,
            boolean s3SelectPushdownEnabled,
            HiveSplitPartitionInfo partitionInfo,
            boolean schedulerUsesHostAddresses,
            Optional<EncryptionInformation> encryptionInformation)
    {
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
        this.inputFormat = requireNonNull(inputFormat, "inputFormat is null");
        this.pathDomain = requireNonNull(pathDomain, "pathDomain is null");
        this.nodeSelectionStrategy = requireNonNull(nodeSelectionStrategy, "nodeSelectionStrategy is null");
        this.s3SelectPushdownEnabled = s3SelectPushdownEnabled;
        this.partitionInfo = partitionInfo;
        this.schedulerUsesHostAddresses = schedulerUsesHostAddresses;
        this.encryptionInformation = requireNonNull(encryptionInformation, "encryptionInformation is null");
        this.minimumTargetSplitSizeInBytes = requireNonNull(minimumTargetSplitSize, "minimumSplittableSize is null").toBytes();
        checkArgument(minimumTargetSplitSizeInBytes > 0, "minimumTargetSplitSize must be > 0, found: %s", minimumTargetSplitSize);
    }

    public Optional<InternalHiveSplit> createInternalHiveSplit(HiveFileInfo fileInfo, boolean splittable)
    {
        return createInternalHiveSplit(fileInfo, OptionalInt.empty(), OptionalInt.empty(), splittable);
    }

    public Optional<InternalHiveSplit> createInternalHiveSplit(HiveFileInfo fileInfo, int readBucketNumber, int tableBucketNumber, boolean splittable)
    {
        return createInternalHiveSplit(fileInfo, OptionalInt.of(readBucketNumber), OptionalInt.of(tableBucketNumber), splittable);
    }

    private Optional<InternalHiveSplit> createInternalHiveSplit(HiveFileInfo fileInfo, OptionalInt readBucketNumber, OptionalInt tableBucketNumber, boolean splittable)
    {
        splittable = splittable &&
                fileInfo.getLength() > minimumTargetSplitSizeInBytes &&
                isSplittable(inputFormat, fileSystem, fileInfo.getPath());
        return createInternalHiveSplit(
                fileInfo.getPath(),
                fileInfo.getBlockLocations(),
                0,
                fileInfo.getLength(),
                fileInfo.getLength(),
                fileInfo.getFileModifiedTime(),
                readBucketNumber,
                tableBucketNumber,
                splittable,
                fileInfo.getExtraFileInfo(),
                ImmutableMap.of());
    }

    public Optional<InternalHiveSplit> createInternalHiveSplit(FileSplit split)
            throws IOException
    {
        FileStatus file = fileSystem.getFileStatus(split.getPath());
        Map<String, String> customSplitInfo = extractCustomSplitInfo(split);
        return createInternalHiveSplit(
                split.getPath(),
                fileSystem.getFileBlockLocations(file, split.getStart(), split.getLength()),
                split.getStart(),
                split.getLength(),
                file.getLen(),
                file.getModificationTime(),
                OptionalInt.empty(),
                OptionalInt.empty(),
                false,
                Optional.empty(),
                customSplitInfo);
    }

    private Optional<InternalHiveSplit> createInternalHiveSplit(
            Path path,
            BlockLocation[] blockLocations,
            long start,
            long length,
            long fileSize,
            long fileModificationTime,
            OptionalInt readBucketNumber,
            OptionalInt tableBucketNumber,
            boolean splittable,
            Optional<byte[]> extraFileInfo,
            Map<String, String> customSplitInfo)
    {
        String pathString = path.toString();
        if (!pathMatchesPredicate(pathDomain, pathString)) {
            return Optional.empty();
        }

        boolean forceLocalScheduling = this.nodeSelectionStrategy == HARD_AFFINITY;
        // For empty files, some filesystem (e.g. LocalFileSystem) produce one empty block
        // while others (e.g. hdfs.DistributedFileSystem) produces no block.
        // Synthesize an empty block if one does not already exist.
        if (fileSize == 0 && blockLocations.length == 0) {
            blockLocations = new BlockLocation[] {new BlockLocation()};
            // Turn off force local scheduling because hosts list doesn't exist.
            forceLocalScheduling = false;
        }

        ImmutableList.Builder<InternalHiveBlock> blockBuilder = ImmutableList.builder();
        for (BlockLocation blockLocation : blockLocations) {
            // clamp the block range
            long blockStart = Math.max(start, blockLocation.getOffset());
            long blockEnd = Math.min(start + length, blockLocation.getOffset() + blockLocation.getLength());
            if (blockStart > blockEnd) {
                // block is outside split range
                continue;
            }
            if (blockStart == blockEnd && !(blockStart == start && blockEnd == start + length)) {
                // skip zero-width block, except in the special circumstance: slice is empty, and the block covers the empty slice interval.
                continue;
            }

            List<HostAddress> addresses = getHostAddresses(blockLocation);
            if (!needsHostAddresses(forceLocalScheduling, addresses)) {
                addresses = ImmutableList.of();
            }
            blockBuilder.add(new InternalHiveBlock(blockEnd, addresses));
        }
        List<InternalHiveBlock> blocks = blockBuilder.build();
        checkBlocks(blocks, start, length);

        if (!splittable) {
            // not splittable, use the hosts from the first block if it exists
            List<HostAddress> addresses = blocks.get(0).getAddresses();
            if (!needsHostAddresses(forceLocalScheduling, addresses)) {
                addresses = ImmutableList.of();
            }
            blocks = ImmutableList.of(new InternalHiveBlock(start + length, addresses));
        }

        URI relativePath = partitionInfo.getPath().relativize(path.toUri());
        return Optional.of(new InternalHiveSplit(
                relativePath.toString(),
                start,
                start + length,
                fileSize,
                fileModificationTime,
                blocks,
                readBucketNumber,
                tableBucketNumber,
                splittable,
                forceLocalScheduling && allBlocksHaveRealAddress(blocks) ? HARD_AFFINITY : nodeSelectionStrategy,
                s3SelectPushdownEnabled && S3SelectPushdown.isCompressionCodecSupported(inputFormat, path),
                partitionInfo,
                extraFileInfo,
                encryptionInformation,
                customSplitInfo));
    }

    private boolean needsHostAddresses(boolean forceLocalScheduling, List<HostAddress> addresses)
    {
        return schedulerUsesHostAddresses || (forceLocalScheduling && hasRealAddress(addresses));
    }

    private static void checkBlocks(List<InternalHiveBlock> blocks, long start, long length)
    {
        checkArgument(length >= 0);
        checkArgument(!blocks.isEmpty());
        checkArgument(start + length == blocks.get(blocks.size() - 1).getEnd());
    }

    private static boolean allBlocksHaveRealAddress(List<InternalHiveBlock> blocks)
    {
        return blocks.stream()
                .map(InternalHiveBlock::getAddresses)
                .allMatch(InternalHiveSplitFactory::hasRealAddress);
    }

    private static boolean hasRealAddress(List<HostAddress> addresses)
    {
        // Hadoop FileSystem returns "localhost" as a default
        return addresses.stream().anyMatch(address -> !address.getHostText().equals("localhost"));
    }

    private static List<HostAddress> getHostAddresses(BlockLocation blockLocation)
    {
        String[] hosts;
        try {
            hosts = blockLocation.getHosts();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return Arrays.stream(hosts)
                .map(HostAddress::fromString)
                .collect(toImmutableList());
    }

    private static boolean pathMatchesPredicate(Optional<Domain> pathDomain, String path)
    {
        if (!pathDomain.isPresent()) {
            return true;
        }

        return pathDomain.get().includesNullableValue(utf8Slice(path));
    }
}
