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
package com.facebook.presto.orc.tools;

import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.orc.FileOrcDataSource;
import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcEncoding;
import com.facebook.presto.orc.OrcPredicate;
import com.facebook.presto.orc.OrcReader;
import com.facebook.presto.orc.OrcRecordReader;
import com.facebook.presto.orc.metadata.OrcType;
import com.facebook.presto.orc.metadata.OrcType.OrcTypeKind;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeParameter;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.type.MapParametricType;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Longs;
import io.airlift.units.DataSize;
import org.joda.time.DateTimeZone;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DecimalType.createDecimalType;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.RowType.field;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

@SuppressWarnings("UseOfSystemOutOrSystemErr")
public final class OrcFileChecksum
{
    private final long rowCount;
    private final List<Long> checksums;

    public static void main(String... args)
            throws Exception
    {
        TypeRegistry typeRegistry = new TypeRegistry();
        new FunctionRegistry(typeRegistry, new BlockEncodingManager(typeRegistry), new FeaturesConfig());

        for (String arg : args) {
            File path = new File(arg).getAbsoluteFile();
            checksumHdfsOrcFile(path, OrcEncoding.DWRF, typeRegistry);
        }
    }

    private static void checksumHdfsOrcFile(File file, OrcEncoding orcEncoding, TypeManager typeManager)
            throws IOException
    {
        OrcDataSource orcDataSource = new FileOrcDataSource(
                file,
                new DataSize(1, MEGABYTE),
                new DataSize(8, MEGABYTE),
                new DataSize(8, MEGABYTE),
                true);

        System.out.print(file.getAbsolutePath());
        try {
            OrcFileChecksum orcFileChecksum = new OrcFileChecksum(orcDataSource, orcEncoding, typeManager);
            System.out.println(" SUCCESS rows: " + orcFileChecksum.getRowCount());
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    public OrcFileChecksum(OrcDataSource orcDataSource, OrcEncoding orcEncoding, TypeManager typeManager)
            throws IOException
    {
        OrcReader orcReader = new OrcReader(orcDataSource, orcEncoding, new DataSize(1, MEGABYTE), new DataSize(8, MEGABYTE), new DataSize(16, MEGABYTE));

        List<Type> types = toPrestoTypes(typeManager, orcReader.getFooter().getTypes());

        ImmutableMap.Builder<Integer, Type> includedColumnsBuilder = ImmutableMap.builder();
        for (int i = 0; i < types.size(); i++) {
            includedColumnsBuilder.put(i, types.get(i));
        }
        Map<Integer, Type> includedColumns = includedColumnsBuilder.build();

        OrcRecordReader recordReader = orcReader.createRecordReader(includedColumns, OrcPredicate.TRUE, DateTimeZone.getDefault(), newSimpleAggregatedMemoryContext());

        long rowCount = 0;
        long[] checksums = new long[types.size()];
        for (int batchSize = recordReader.nextBatch(); batchSize > 0; batchSize = recordReader.nextBatch()) {
            readBatch(recordReader, types, checksums, batchSize);
            rowCount += batchSize;
        }
        this.rowCount = rowCount;
        this.checksums = ImmutableList.copyOf(Longs.asList(checksums));
    }

    private static List<Type> toPrestoTypes(TypeManager typeManager, List<OrcType> orcTypes)
    {
        OrcType tableType = orcTypes.get(0);
        checkArgument(tableType.getOrcTypeKind() == OrcTypeKind.STRUCT);
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (int i = 0; i < tableType.getFieldCount(); i++) {
            types.add(toPrestoType(typeManager, orcTypes.get(tableType.getFieldTypeIndex(i)), orcTypes));
        }
        return types.build();
    }

    private static Type toPrestoType(TypeManager typeManager, OrcType orcType, List<OrcType> orcTypes)
    {
        switch (orcType.getOrcTypeKind()) {
            case BOOLEAN:
                return BOOLEAN;
            case BYTE:
                return TINYINT;
            case SHORT:
                return SMALLINT;
            case INT:
                return INTEGER;
            case LONG:
                return BIGINT;
            case DECIMAL:
                return createDecimalType(orcType.getPrecision().get(), orcType.getScale().get());
            case FLOAT:
                return REAL;
            case DOUBLE:
                return DOUBLE;
            case STRING:
            case VARCHAR:
            case CHAR:
                return VARCHAR;
            case BINARY:
                return VARBINARY;
            case DATE:
                return DATE;
            case TIMESTAMP:
                return TIMESTAMP;
            case LIST:
                return new ArrayType(toPrestoType(typeManager, orcTypes.get(orcType.getFieldTypeIndex(0)), orcTypes));
            case MAP:
                return MapParametricType.MAP.createType(typeManager, ImmutableList.of(
                        TypeParameter.of(toPrestoType(typeManager, orcTypes.get(orcType.getFieldTypeIndex(0)), orcTypes)),
                        TypeParameter.of(toPrestoType(typeManager, orcTypes.get(orcType.getFieldTypeIndex(1)), orcTypes))));
            case STRUCT:
                return RowType.from(
                        IntStream.range(0, orcType.getFieldCount())
                                .mapToObj(i -> field(orcType.getFieldName(i), toPrestoType(typeManager, orcTypes.get(orcType.getFieldTypeIndex(i)), orcTypes)))
                                .collect(toImmutableList()));
            default:
                throw new IllegalArgumentException("Unsupported type kind: " + orcType.getOrcTypeKind());
        }
    }

    public long getRowCount()
    {
        return rowCount;
    }

    public List<Long> getChecksums()
    {
        return checksums;
    }

    private static void readBatch(OrcRecordReader recordReader, List<Type> columnTypes, long[] checksums, int batchSize)
            throws IOException
    {
        for (int columnIndex = 0; columnIndex < checksums.length; columnIndex++) {
            Type type = columnTypes.get(columnIndex);
            Block block = recordReader.readBlock(type, columnIndex);
            verify(batchSize == block.getPositionCount());
            for (int position = 0; position < block.getPositionCount(); position++) {
                long valueHash = hashBlockPosition(type, block, position);
                checksums[columnIndex] = combineHash(checksums[columnIndex], valueHash);
            }
        }
    }

    private static long combineHash(long currentHash, long valueHash)
    {
        return 31 * currentHash + valueHash;
    }

    private static long hashBlockPosition(Type type, Block block, int position)
    {
        if (block.isNull(position)) {
            return 0;
        }
        return type.hash(block, position);
    }
}
