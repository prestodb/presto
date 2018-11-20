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

import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HivePartitionKey;
import com.facebook.presto.hive.HiveSplit.BucketConversion;
import com.facebook.presto.hive.HiveTypeName;
import com.facebook.presto.hive.InternalHiveSplit;
import com.facebook.presto.hive.InternalHiveSplit.InternalHiveBlock;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;

import static com.facebook.presto.hive.HiveColumnHandle.isPathColumnHandle;
import static com.facebook.presto.hive.HiveUtil.isSplittable;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static java.util.Objects.requireNonNull;

public class InternalHiveSplitFactory
{
    private final FileSystem fileSystem;
    private final String partitionName;
    private final InputFormat<?, ?> inputFormat;
    private final Properties schema;
    private final List<HivePartitionKey> partitionKeys;
    private final Optional<Domain> pathDomain;
    private final Map<Integer, HiveTypeName> columnCoercions;
    private final Optional<BucketConversion> bucketConversion;
    private final boolean forceLocalScheduling;

    public InternalHiveSplitFactory(
            FileSystem fileSystem,
            String partitionName,
            InputFormat<?, ?> inputFormat,
            Properties schema,
            List<HivePartitionKey> partitionKeys,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            Map<Integer, HiveTypeName> columnCoercions,
            Optional<BucketConversion> bucketConversion,
            boolean forceLocalScheduling)
    {
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
        this.partitionName = requireNonNull(partitionName, "partitionName is null");
        this.inputFormat = requireNonNull(inputFormat, "inputFormat is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.partitionKeys = requireNonNull(partitionKeys, "partitionKeys is null");
        pathDomain = getPathDomain(requireNonNull(effectivePredicate, "effectivePredicate is null"));
        this.columnCoercions = requireNonNull(columnCoercions, "columnCoercions is null");
        this.bucketConversion = requireNonNull(bucketConversion, "bucketConversion is null");
        this.forceLocalScheduling = forceLocalScheduling;
    }

    public String getPartitionName()
    {
        return partitionName;
    }

    public Optional<InternalHiveSplit> createInternalHiveSplit(LocatedFileStatus status, boolean splittable)
    {
        return createInternalHiveSplit(status, OptionalInt.empty(), splittable);
    }

    public Optional<InternalHiveSplit> createInternalHiveSplit(LocatedFileStatus status, int bucketNumber)
    {
        return createInternalHiveSplit(status, OptionalInt.of(bucketNumber), false);
    }

    private Optional<InternalHiveSplit> createInternalHiveSplit(LocatedFileStatus status, OptionalInt bucketNumber, boolean splittable)
    {
        splittable = splittable && isSplittable(inputFormat, fileSystem, status.getPath());
        return createInternalHiveSplit(
                status.getPath(),
                status.getBlockLocations(),
                0,
                status.getLen(),
                status.getLen(),
                bucketNumber,
                splittable);
    }

    public Optional<InternalHiveSplit> createInternalHiveSplit(FileSplit split)
            throws IOException
    {
        FileStatus file = fileSystem.getFileStatus(split.getPath());
        return createInternalHiveSplit(
                split.getPath(),
                fileSystem.getFileBlockLocations(file, split.getStart(), split.getLength()),
                split.getStart(),
                split.getLength(),
                file.getLen(),
                OptionalInt.empty(),
                false);
    }

    private Optional<InternalHiveSplit> createInternalHiveSplit(
            Path path,
            BlockLocation[] blockLocations,
            long start,
            long length,
            long fileSize,
            OptionalInt bucketNumber,
            boolean splittable)
    {
        String pathString = path.toString();
        if (!pathMatchesPredicate(pathDomain, pathString)) {
            return Optional.empty();
        }

        boolean forceLocalScheduling = this.forceLocalScheduling;

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
            blockBuilder.add(new InternalHiveBlock(blockStart, blockEnd, getHostAddresses(blockLocation)));
        }
        List<InternalHiveBlock> blocks = blockBuilder.build();
        checkBlocks(blocks, start, length);

        if (!splittable) {
            // not splittable, use the hosts from the first block if it exists
            blocks = ImmutableList.of(new InternalHiveBlock(start, start + length, blocks.get(0).getAddresses()));
        }

        return Optional.of(new InternalHiveSplit(
                partitionName,
                pathString,
                start,
                start + length,
                fileSize,
                schema,
                partitionKeys,
                blocks,
                bucketNumber,
                splittable,
                forceLocalScheduling && allBlocksHaveRealAddress(blocks),
                columnCoercions,
                bucketConversion));
    }

    private static void checkBlocks(List<InternalHiveBlock> blocks, long start, long length)
    {
        checkArgument(length >= 0);
        checkArgument(!blocks.isEmpty());
        checkArgument(start == blocks.get(0).getStart());
        checkArgument(start + length == blocks.get(blocks.size() - 1).getEnd());
        for (int i = 1; i < blocks.size(); i++) {
            checkArgument(blocks.get(i - 1).getEnd() == blocks.get(i).getStart());
        }
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

    private static Optional<Domain> getPathDomain(TupleDomain<HiveColumnHandle> effectivePredicate)
    {
        if (!effectivePredicate.getDomains().isPresent()) {
            return Optional.empty();
        }

        return effectivePredicate.getDomains().get().entrySet().stream()
                .filter(entry -> isPathColumnHandle(entry.getKey()))
                .findFirst()
                .map(Map.Entry::getValue);
    }

    private static boolean pathMatchesPredicate(Optional<Domain> pathDomain, String path)
    {
        if (!pathDomain.isPresent()) {
            return true;
        }

        return pathDomain.get().includesNullableValue(utf8Slice(path));
    }
}
