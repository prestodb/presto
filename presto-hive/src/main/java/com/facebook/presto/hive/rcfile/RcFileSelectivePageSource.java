package com.facebook.presto.hive.rcfile;

import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.orc.FilterFunction;
import com.facebook.presto.orc.TupleDomainFilter;
import com.facebook.presto.orc.TupleDomainFilterUtils;
import com.facebook.presto.rcfile.RcFileReader;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.VarcharType;
import io.airlift.slice.Slice;
import org.joda.time.DateTimeZone;

import java.util.Collection;
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
    private final List<HiveColumnHandle> columns;
    private TupleDomain<HiveColumnHandle> predicate;
    private TypeManager typeManager;
    private List<FilterFunction> filterFunctions;

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
    }

    public Page getNextPage()
    {
        Page page = super.getNextPage();
        if (page == null || page.getPositionCount() == 0 || this.predicate == null) {
            return page;
        }
        int[] positions = new int[page.getPositionCount()];
        RuntimeException[] errors = new RuntimeException[page.getPositionCount()];
        Map<HiveColumnHandle, Domain> columnDomains = this.predicate.getDomains().get();
        Collection<HiveColumnHandle> columnIndices = columnDomains.keySet();
        int positionCount = positions.length;

        for (HiveColumnHandle col : columnIndices) {
            positionCount = filterBlock(page.getBlock(col.getHiveColumnIndex()), col.getHiveType().getType(typeManager), TupleDomainFilterUtils.toFilter(columnDomains.get(col)), positions, positionCount);
        }

        for (FilterFunction function : filterFunctions) {
            Block[] inputBlocks = new Block[page.getChannelCount()];

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
                        positions[i] = position;
                        livePositionCount++;
                    }
                }
                else if (filter.testLong(type.getLong(block, position))) {
                    positions[i] = position;
                    livePositionCount++;
                }
            }
        }
        else if (type == DoubleType.DOUBLE) {
            for (int i = 0; i < positionCount; i++) {
                int position = positions[i];
                if (block.isNull(position)) {
                    if (filter.testNull()) {
                        positions[i] = position;
                        livePositionCount++;
                    }
                }
                else if (filter.testDouble(longBitsToDouble(block.getLong(position)))) {
                    positions[i] = position;
                    livePositionCount++;
                }
            }
        }
        else if (type == REAL) {
            for (int i = 0; i < positionCount; i++) {
                int position = positions[i];
                if (block.isNull(position)) {
                    if (filter.testNull()) {
                        positions[i] = position;
                        livePositionCount++;
                    }
                }
                else if (filter.testFloat(intBitsToFloat(block.getInt(position)))) {
                    positions[i] = position;
                    livePositionCount++;
                }
            }
        }
        else if (isVarcharType(type)) {
            for (int i = 0; i < positionCount; i++) {
                int position = positions[i];
                if (block.isNull(position)) {
                    if (filter.testNull()) {
                        positions[i] = position;
                        livePositionCount++;
                    }
                }
                else {
                    Slice slice = block.getSlice(position, 0, block.getSliceLength(position));
                    if (filter.testBytes((byte[]) slice.getBase(), (int) slice.getAddress() - ARRAY_BYTE_BASE_OFFSET, slice.length())) {
                        positions[i] = position;
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
}
