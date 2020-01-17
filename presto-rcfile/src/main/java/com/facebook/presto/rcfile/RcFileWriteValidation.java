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
package com.facebook.presto.rcfile;

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.slice.XxHash64;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class RcFileWriteValidation
{
    private final byte version;
    private final Map<String, String> metadata;
    private final Optional<String> codecClassName;
    private final long syncFirst;
    private final long syncSecond;
    private final WriteChecksum checksum;

    private RcFileWriteValidation(byte version, Map<String, String> metadata, Optional<String> codecClassName, long syncFirst, long syncSecond, WriteChecksum checksum)
    {
        this.version = version;
        this.metadata = ImmutableMap.copyOf(requireNonNull(metadata, "metadata is null"));
        this.codecClassName = requireNonNull(codecClassName, "codecClassName is null");
        this.syncFirst = syncFirst;
        this.syncSecond = syncSecond;
        this.checksum = requireNonNull(checksum, "checksum is null");
    }

    public byte getVersion()
    {
        return version;
    }

    public Map<String, String> getMetadata()
    {
        return metadata;
    }

    public Optional<String> getCodecClassName()
    {
        return codecClassName;
    }

    public long getSyncFirst()
    {
        return syncFirst;
    }

    public long getSyncSecond()
    {
        return syncSecond;
    }

    public WriteChecksum getChecksum()
    {
        return checksum;
    }

    public static class WriteChecksum
    {
        private final long totalRowCount;
        private final long rowGroupHash;
        private final List<Long> columnHashes;

        public WriteChecksum(long totalRowCount, long rowGroupHash, List<Long> columnHashes)
        {
            this.totalRowCount = totalRowCount;
            this.rowGroupHash = rowGroupHash;
            this.columnHashes = columnHashes;
        }

        public long getTotalRowCount()
        {
            return totalRowCount;
        }

        public long getRowGroupHash()
        {
            return rowGroupHash;
        }

        public List<Long> getColumnHashes()
        {
            return columnHashes;
        }
    }

    public static class WriteChecksumBuilder
    {
        // This value is a large arbitrary prime
        private static final long NULL_HASH_CODE = 0x6e3efbd56c16a0cbL;

        private final List<Type> types;
        private long totalRowCount;
        private final List<XxHash64> columnHashes;
        private final XxHash64 rowGroupHash = new XxHash64();

        private final byte[] longBuffer = new byte[Long.BYTES];
        private final Slice longSlice = Slices.wrappedBuffer(longBuffer);

        private WriteChecksumBuilder(List<Type> types)
        {
            this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));

            ImmutableList.Builder<XxHash64> columnHashes = ImmutableList.builder();
            for (Type ignored : types) {
                columnHashes.add(new XxHash64());
            }
            this.columnHashes = columnHashes.build();
        }

        public static WriteChecksumBuilder createWriteChecksumBuilder(Map<Integer, Type> readColumns)
        {
            requireNonNull(readColumns, "readColumns is null");
            checkArgument(!readColumns.isEmpty(), "readColumns is empty");
            int columnCount = readColumns.keySet().stream()
                    .mapToInt(Integer::intValue)
                    .max().getAsInt() + 1;
            checkArgument(readColumns.size() == columnCount, "checksum requires all columns to be read");

            ImmutableList.Builder<Type> types = ImmutableList.builder();
            for (int column = 0; column < columnCount; column++) {
                Type type = readColumns.get(column);
                checkArgument(type != null, "checksum requires all columns to be read");
                types.add(type);
            }
            return new WriteChecksumBuilder(types.build());
        }

        public void addRowGroup(int rowCount)
        {
            longSlice.setInt(0, rowCount);
            rowGroupHash.update(longBuffer, 0, Integer.BYTES);
        }

        public void addPage(Page page)
        {
            requireNonNull(page, "page is null");
            checkArgument(page.getChannelCount() == columnHashes.size(), "invalid page");

            totalRowCount += page.getPositionCount();
            for (int channel = 0; channel < columnHashes.size(); channel++) {
                Type type = types.get(channel);
                Block block = page.getBlock(channel);
                XxHash64 xxHash64 = columnHashes.get(channel);
                for (int position = 0; position < block.getPositionCount(); position++) {
                    long hash = hashPositionSkipNullMapKeys(type, block, position);
                    longSlice.setLong(0, hash);
                    xxHash64.update(longBuffer);
                }
            }
        }

        private static long hashPositionSkipNullMapKeys(Type type, Block block, int position)
        {
            if (block.isNull(position)) {
                return NULL_HASH_CODE;
            }

            if (type.getTypeSignature().getBase().equals(StandardTypes.MAP)) {
                Type keyType = type.getTypeParameters().get(0);
                Type valueType = type.getTypeParameters().get(1);
                Block mapBlock = (Block) type.getObject(block, position);
                long hash = 0;
                for (int i = 0; i < mapBlock.getPositionCount(); i += 2) {
                    if (!mapBlock.isNull(i)) {
                        hash += hashPositionSkipNullMapKeys(keyType, mapBlock, i);
                        hash += hashPositionSkipNullMapKeys(valueType, mapBlock, i + 1);
                    }
                }
                return hash;
            }

            if (type.getTypeSignature().getBase().equals(StandardTypes.ARRAY)) {
                Type elementType = type.getTypeParameters().get(0);
                Block array = (Block) type.getObject(block, position);
                long hash = 0;
                for (int i = 0; i < array.getPositionCount(); i++) {
                    hash = 31 * hash + hashPositionSkipNullMapKeys(elementType, array, i);
                }
                return hash;
            }

            if (type.getTypeSignature().getBase().equals(StandardTypes.ROW)) {
                Block row = (Block) type.getObject(block, position);
                long hash = 0;
                for (int i = 0; i < row.getPositionCount(); i++) {
                    Type elementType = type.getTypeParameters().get(i);
                    hash = 31 * hash + hashPositionSkipNullMapKeys(elementType, row, i);
                }
                return hash;
            }

            return type.hash(block, position);
        }

        public WriteChecksum build()
        {
            return new WriteChecksum(
                    totalRowCount,
                    rowGroupHash.hash(),
                    columnHashes.stream()
                            .map(XxHash64::hash)
                            .collect(toList()));
        }
    }

    public static class RcFileWriteValidationBuilder
    {
        private byte version;
        private final Map<String, String> metadata = new HashMap<>();
        private Optional<String> codecClassName;
        private long syncFirst;
        private long syncSecond;
        private final WriteChecksumBuilder checksum;

        public RcFileWriteValidationBuilder(List<Type> types)
        {
            this.checksum = new WriteChecksumBuilder(types);
        }

        public RcFileWriteValidationBuilder setVersion(byte version)
        {
            this.version = version;
            return this;
        }

        public RcFileWriteValidationBuilder addMetadataProperty(String key, String value)
        {
            metadata.put(key, value);
            return this;
        }

        public RcFileWriteValidationBuilder setCodecClassName(Optional<String> codecClassName)
        {
            this.codecClassName = codecClassName;
            return this;
        }

        public RcFileWriteValidationBuilder setSyncFirst(long syncFirst)
        {
            this.syncFirst = syncFirst;
            return this;
        }

        public RcFileWriteValidationBuilder setSyncSecond(long syncSecond)
        {
            this.syncSecond = syncSecond;
            return this;
        }

        public RcFileWriteValidationBuilder addRowGroup(int rowCount)
        {
            checksum.addRowGroup(rowCount);
            return this;
        }

        public RcFileWriteValidationBuilder addPage(Page page)
        {
            checksum.addPage(page);
            return this;
        }

        public RcFileWriteValidation build()
        {
            return new RcFileWriteValidation(version, metadata, codecClassName, syncFirst, syncSecond, checksum.build());
        }
    }
}
