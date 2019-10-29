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
package com.facebook.presto.hive.rcfile;

import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.orc.FilterFunction;
import com.facebook.presto.orc.TupleDomainFilter;
import com.facebook.presto.orc.TupleDomainFilterUtils;
import com.facebook.presto.rcfile.RcFileReader;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.joda.time.DateTimeZone;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static java.lang.Double.longBitsToDouble;
import static java.lang.Float.intBitsToFloat;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

public class RcFileSelectivePageSource
        extends RcFilePageSource
{
    private static final int NULL_ENTRY_SIZE = 0;

    private final List<HiveColumnHandle> columns;
    private TupleDomain<HiveColumnHandle> predicate;
    private TypeManager typeManager;
    private List<FilterFunction> filterFunctions;
    private final Block[] constantBlocks;
    private final int[] hiveColumnIndexes;
    private final List<String> columnNames;
    private final List<Type> types;
    private final Map<Integer, Integer> columnMapper;



    public RcFileSelectivePageSource(
            RcFileReader rcFileReader,
            List<HiveColumnHandle> columns,
            DateTimeZone hiveStorageTimeZone,
            TypeManager typeManager,
            ConnectorSession session,
            TupleDomain<HiveColumnHandle> predicate,
            List<FilterFunction> remainingPredicate)
    {
        super(rcFileReader, columns, hiveStorageTimeZone, typeManager, session);
        this.columns = columns;
        this.predicate = predicate;
        this.typeManager = typeManager;
        this.filterFunctions = remainingPredicate;
        int size = columns.size();

        this.constantBlocks = new Block[size];
        this.hiveColumnIndexes = new int[size];
        this.columnMapper = new HashMap<>();

        ImmutableList.Builder<String> namesBuilder = ImmutableList.builder();
        ImmutableList.Builder<Type> typesBuilder = ImmutableList.builder();
        ImmutableList.Builder<HiveType> hiveTypesBuilder = ImmutableList.builder();
        for (int columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
            HiveColumnHandle column = columns.get(columnIndex);

            String name = column.getName();
            Type type = typeManager.getType(column.getTypeSignature());

            namesBuilder.add(name);
            typesBuilder.add(type);
            hiveTypesBuilder.add(column.getHiveType());

            hiveColumnIndexes[columnIndex] = column.getHiveColumnIndex();
            this.columnMapper.put(column.getHiveColumnIndex(), columnIndex);

            if (hiveColumnIndexes[columnIndex] >= rcFileReader.getColumnCount()) {
                // this file may contain fewer fields than what's declared in the schema
                // this happens when additional columns are added to the hive table after files have been created
                BlockBuilder blockBuilder = type.createBlockBuilder(null, 1, NULL_ENTRY_SIZE);
                blockBuilder.appendNull();
                constantBlocks[columnIndex] = blockBuilder.build();
            }
        }
        types = typesBuilder.build();
        columnNames = namesBuilder.build();
    }

    public Page getNextPage()
    {
        Page page = super.getNextPage();
        if (page == null || page.getPositionCount() == 0 || this.predicate == null) {
            return page;
        }

        RuntimeException[] errors = new RuntimeException[page.getPositionCount()];
        Map<HiveColumnHandle, Domain> columnDomains = this.predicate.getDomains().get();
        Collection<HiveColumnHandle> columnIndices = columnDomains.keySet();

        if (columnIndices.size() == 0 && this.filterFunctions.size() == 0) {
            return page;
        }

        int[] positions = new int[page.getPositionCount()];
        for (int i = 0; i < positions.length; i++) {
            positions[i] = i;
        }
        int positionCount = positions.length;

        for (HiveColumnHandle col : columnIndices) {
            if (col.getHiveColumnIndex() >= 0) {
                positionCount = filterBlock(page.getBlock(columnMapper.get(col.getHiveColumnIndex())), col.getHiveType().getType(typeManager), TupleDomainFilterUtils.toFilter(columnDomains.get(col)), positions, positionCount);
            }
        }

        for (FilterFunction function : filterFunctions) {
            Block[] inputBlocks = new Block[function.getInputChannels().length];

            for (int i = 0; i < page.getChannelCount(); i++) {
                inputBlocks[i] = page.getBlock(i);
            }

            page = new Page(positionCount, inputBlocks);
            positionCount = function.filter(page, positions, positionCount, errors);
            if (positionCount == 0) {
                break;
            }
        }

        for (int i = 0; i < positionCount; i++) {
            if (errors[i] != null) {
                throw errors[i];
            }
        }

        return page.getPositions(positions, 0, positionCount);
    }

    public static int filterBlock(Block block, Type type, TupleDomainFilter filter, int[] positions, int positionCount)
    {
        int livePositionCount = 0;
        if (type == BIGINT || type == INTEGER || type == SMALLINT || type == TINYINT) {
            for (int i = 0; i < positionCount; i++) {
                int position = positions[i];
                if (block.isNull(position)) {
                    if (filter.testNull()) {
                        positions[livePositionCount] = position;
                        livePositionCount++;
                    }
                }
                else if (filter.testLong(type.getLong(block, position))) {
                    positions[livePositionCount] = position;
                    livePositionCount++;
                }
            }
        }
        else if (type == DoubleType.DOUBLE) {
            for (int i = 0; i < positionCount; i++) {
                int position = positions[i];
                if (block.isNull(position)) {
                    if (filter.testNull()) {
                        positions[livePositionCount] = position;
                        livePositionCount++;
                    }
                }
                else if (filter.testDouble(longBitsToDouble(block.getLong(position)))) {
                    positions[livePositionCount] = position;
                    livePositionCount++;
                }
            }
        }
        else if (type == REAL) {
            for (int i = 0; i < positionCount; i++) {
                int position = positions[i];
                if (block.isNull(position)) {
                    if (filter.testNull()) {
                        positions[livePositionCount] = position;
                        livePositionCount++;
                    }
                }
                else if (filter.testFloat(intBitsToFloat(block.getInt(position)))) {
                    positions[livePositionCount] = position;
                    livePositionCount++;
                }
            }
        }
        else if (isDecimalType(type)) {
            for (int i = 0; i < positionCount; i++) {
                int position = positions[i];
                if (block.isNull(position)) {
                    if (filter.testNull()) {
                        positions[livePositionCount] = position;
                        livePositionCount++;
                    }
                }
                else {
                    if (((DecimalType) type).isShort()) {
                        if (filter.testLong(block.getLong(position))) {
                            positions[livePositionCount] = position;
                            livePositionCount++;
                        }
                    }
                    else if (filter.testDecimal(block.getLong(position, 0), block.getLong(position, Long.BYTES))) {
                        positions[livePositionCount] = position;
                        livePositionCount++;
                    }
                }
            }
        }
        else if (isVarcharType(type)) {
            for (int i = 0; i < positionCount; i++) {
                int position = positions[i];
                if (block.isNull(position)) {
                    if (filter.testNull()) {
                        positions[livePositionCount] = position;
                        livePositionCount++;
                    }
                }
                else {
                    Slice slice = block.getSlice(position, 0, block.getSliceLength(position));
                    if (filter.testBytes((byte[]) slice.getBase(), (int) slice.getAddress() - ARRAY_BYTE_BASE_OFFSET, slice.length())) {
                        positions[livePositionCount] = position;
                        livePositionCount++;
                    }
                }
            }
        }
        else if (isCharType(type)) {
            for (int i = 0; i < positionCount; i++) {
                int position = positions[i];
                if (block.isNull(position)) {
                    if (filter.testNull()) {
                        positions[livePositionCount] = position;
                        livePositionCount++;
                    }
                }
                else {
                    Slice slice = block.getSlice(position, 0, block.getSliceLength(position));
                    if (filter.testBytes((byte[]) slice.getBase(), (int) slice.getAddress() - ARRAY_BYTE_BASE_OFFSET, slice.length())) {
                        positions[livePositionCount] = position;
                        livePositionCount++;
                    }
                }
            }
        }
        else {
            throw new UnsupportedOperationException("BlockStreamReadre of " + type.toString() + " not supported");
        }

        return livePositionCount;
    }

    public static boolean isVarcharType(Type type)
    {
        return type instanceof VarcharType;
    }

    public static boolean isCharType(Type type)
    {
        return type instanceof CharType;
    }

    public static boolean isDecimalType(Type type)
    {
        return type instanceof DecimalType;
    }
}
