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
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.github.luben.zstd.Zstd;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.LongStream;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.hive.HiveErrorCode.MALFORMED_HIVE_FILE_STATISTICS;
import static com.facebook.presto.hive.HiveSessionProperties.isFileRenamingEnabled;
import static com.facebook.presto.hive.PartitionUpdate.FileWriteInfo;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.ISO_8859_1;

public class HiveManifestUtils
{
    private static final int FILE_SIZE_CHANNEL = 0;
    private static final int ROW_COUNT_CHANNEL = 1;
    private static final int COMPRESSION_LEVEL = 7; // default level
    private static final String COMMA = ",";

    public static final String FILE_NAMES = "FILE_NAMES";
    public static final String FILE_SIZES = "FILE_SIZES";
    public static final String MANIFEST_VERSION = "MANIFEST_VERSION";
    public static final String VERSION_1 = "V1";

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

    public static Map<String, String> updatePartitionMetadataWithFileNamesAndSizes(PartitionUpdate partitionUpdate, Map<String, String> metadata)
    {
        ImmutableMap.Builder<String, String> partitionMetadata = ImmutableMap.builder();
        List<FileWriteInfo> fileWriteInfos = new ArrayList<>(partitionUpdate.getFileWriteInfos());

        if (!partitionUpdate.containsNumberedFileNames()) {
            // Filenames starting with ".tmp.presto" will be renamed in TableFinishOperator. So it doesn't make sense to store the filenames in manifest
            return metadata;
        }

        // Sort the file infos based on fileName
        fileWriteInfos.sort(Comparator.comparing(info -> Integer.valueOf(info.getWriteFileName())));

        List<String> fileNames = fileWriteInfos.stream().map(FileWriteInfo::getWriteFileName).collect(toImmutableList());
        List<Long> fileSizes = fileWriteInfos.stream().map(FileWriteInfo::getFileSize).filter(Optional::isPresent).map(Optional::get).collect(toImmutableList());

        if (fileSizes.size() < fileNames.size()) {
            if (fileSizes.isEmpty()) {
                // These files may not have been written by OrcFileWriter. So file sizes not available.
                return metadata;
            }
            throw new PrestoException(
                    MALFORMED_HIVE_FILE_STATISTICS,
                    format(
                            "During manifest creation for partition= %s, filename count= %s is not equal to filesizes count= %s",
                            partitionUpdate.getName(),
                            fileNames.size(),
                            fileSizes.size()));
        }

        // Compress the file names into a consolidated string
        partitionMetadata.put(FILE_NAMES, compressFileNames(fileNames));

        // Compress the file sizes
        partitionMetadata.put(FILE_SIZES, compressFileSizes(fileSizes));

        partitionMetadata.put(MANIFEST_VERSION, VERSION_1);
        partitionMetadata.putAll(metadata);

        return partitionMetadata.build();
    }

    public static OptionalLong getManifestSizeInBytes(ConnectorSession session, PartitionUpdate partitionUpdate, Map<String, String> parameters)
    {
        if (isFileRenamingEnabled(session) && partitionUpdate.containsNumberedFileNames()) {
            if (parameters.containsKey(MANIFEST_VERSION)) {
                return OptionalLong.of(parameters.get(FILE_NAMES).length() + parameters.get(FILE_SIZES).length());
            }
            List<FileWriteInfo> fileWriteInfos = partitionUpdate.getFileWriteInfos();
            return OptionalLong.of(compressFileNames(fileWriteInfos.stream().map(FileWriteInfo::getWriteFileName).collect(toImmutableList())).length()
                    + compressFileSizes(fileWriteInfos.stream().map(FileWriteInfo::getFileSize).filter(Optional::isPresent).map(Optional::get).collect(toImmutableList())).length());
        }

        return OptionalLong.empty();
    }

    static String compressFileNames(List<String> fileNames)
    {
        if (fileNames.size() == 1) {
            return fileNames.get(0);
        }

        boolean isContinuousSequence = true;
        int start = 0;
        for (String name : fileNames) {
            if (start != Integer.parseInt(name)) {
                isContinuousSequence = false;
                break;
            }
            start++;
        }

        if (isContinuousSequence) {
            return fileNames.get(fileNames.size() - 1);
        }

        return compressFileNamesUsingRoaringBitmap(fileNames);
    }

    static List<String> decompressFileNames(String compressedFileNames)
    {
        // Check if the compressed fileNames string is a number
        if (compressedFileNames.matches("\\d+")) {
            long end = Long.parseLong(compressedFileNames);

            if (end == 0) {
                return ImmutableList.of("0");
            }

            return LongStream.range(0, end + 1).mapToObj(String::valueOf).collect(toImmutableList());
        }

        try {
            RoaringBitmap roaringBitmap = new RoaringBitmap();
            ByteBuffer byteBuffer = ByteBuffer.wrap(compressedFileNames.getBytes(ISO_8859_1));
            roaringBitmap.deserialize(byteBuffer);
            return Arrays.stream(roaringBitmap.toArray()).mapToObj(Integer::toString).collect(toImmutableList());
        }
        catch (IOException e) {
            throw new PrestoException(MALFORMED_HIVE_FILE_STATISTICS, "Failed de-compressing the file names in manifest");
        }
    }

    private static String compressFileNamesUsingRoaringBitmap(List<String> fileNames)
    {
        RoaringBitmap roaringBitmap = new RoaringBitmap();

        // Add file names to roaring bitmap
        fileNames.forEach(name -> roaringBitmap.add(Integer.parseInt(name)));

        // Serialize the compressed data into ByteBuffer
        ByteBuffer byteBuffer = ByteBuffer.allocate(roaringBitmap.serializedSizeInBytes());
        roaringBitmap.serialize(byteBuffer);
        byteBuffer.flip();

        return new String(byteBuffer.array(), ISO_8859_1);
    }

    public static String compressFileSizes(List<Long> fileSizes)
    {
        String fileSizesString = Joiner.on(COMMA).join(fileSizes.stream().map(String::valueOf).collect(toImmutableList()));
        try {
            return new String(Zstd.compress(fileSizesString.getBytes(ISO_8859_1), COMPRESSION_LEVEL), ISO_8859_1);
        }
        catch (RuntimeException e) {
            throw new PrestoException(MALFORMED_HIVE_FILE_STATISTICS, "Failed compressing the file sizes for manifest");
        }
    }

    public static List<Long> decompressFileSizes(String compressedFileSizes)
    {
        try {
            byte[] compressedBytes = compressedFileSizes.getBytes(ISO_8859_1);
            long decompressedSize = Zstd.decompressedSize(compressedBytes);
            String decompressedFileSizes = new String(Zstd.decompress(compressedBytes, (int) decompressedSize), ISO_8859_1);
            return Arrays.stream(decompressedFileSizes.split(COMMA)).map(Long::valueOf).collect(toImmutableList());
        }
        catch (RuntimeException e) {
            throw new PrestoException(MALFORMED_HIVE_FILE_STATISTICS, "Failed de-compressing the file sizes in manifest");
        }
    }
}
