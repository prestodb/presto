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

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.EncryptionInformation;
import com.facebook.presto.hive.FileFormatDataSourceStats;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveBatchPageSourceFactory;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HiveFileContext;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.rcfile.AircompressorCodecFactory;
import com.facebook.presto.rcfile.HadoopCodecFactory;
import com.facebook.presto.rcfile.RcFileCorruptionException;
import com.facebook.presto.rcfile.RcFileEncoding;
import com.facebook.presto.rcfile.RcFileReader;
import com.facebook.presto.rcfile.binary.BinaryRcFileEncoding;
import com.facebook.presto.rcfile.text.TextRcFileEncoding;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.AGGREGATED;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_BAD_DATA;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_MISSING_DATA;
import static com.facebook.presto.hive.metastore.MetastoreUtil.getHiveSchema;
import static com.facebook.presto.rcfile.text.TextRcFileEncoding.DEFAULT_NULL_SEQUENCE;
import static com.facebook.presto.rcfile.text.TextRcFileEncoding.DEFAULT_SEPARATORS;
import static com.google.common.base.Strings.nullToEmpty;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.hive.serde.serdeConstants.COLLECTION_DELIM;
import static org.apache.hadoop.hive.serde.serdeConstants.ESCAPE_CHAR;
import static org.apache.hadoop.hive.serde.serdeConstants.FIELD_DELIM;
import static org.apache.hadoop.hive.serde.serdeConstants.MAPKEY_DELIM;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_FORMAT;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LAST_COLUMN_TAKES_REST;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_NULL_FORMAT;
import static org.apache.hadoop.hive.serde2.lazy.LazySerDeParameters.SERIALIZATION_EXTEND_NESTING_LEVELS;
import static org.apache.hadoop.hive.serde2.lazy.LazyUtils.getByte;

public class RcFilePageSourceFactory
        implements HiveBatchPageSourceFactory
{
    private static final int TEXT_LEGACY_NESTING_LEVELS = 8;
    private static final int TEXT_EXTENDED_NESTING_LEVELS = 29;

    private final TypeManager typeManager;
    private final HdfsEnvironment hdfsEnvironment;
    private final FileFormatDataSourceStats stats;

    @Inject
    public RcFilePageSourceFactory(TypeManager typeManager, HdfsEnvironment hdfsEnvironment, FileFormatDataSourceStats stats)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.stats = requireNonNull(stats, "stats is null");
    }

    @Override
    public Optional<? extends ConnectorPageSource> createPageSource(
            Configuration configuration,
            ConnectorSession session,
            Path path,
            long start,
            long length,
            long fileSize,
            Storage storage,
            SchemaTableName tableName,
            Map<String, String> tableParameters,
            List<HiveColumnHandle> columns,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            DateTimeZone hiveStorageTimeZone,
            HiveFileContext hiveFileContext,
            Optional<EncryptionInformation> encryptionInformation)
    {
        if (!columns.isEmpty() && columns.stream().allMatch(hiveColumnHandle -> hiveColumnHandle.getColumnType() == AGGREGATED)) {
            throw new UnsupportedOperationException("Partial aggregation pushdown only supported for ORC/Parquet files. " +
                    "Table " + tableName.toString() + " has file (" + path.toString() + ") of format " + storage.getStorageFormat().getOutputFormat() +
                    ". Set session property hive.pushdown_partial_aggregations_into_scan=false and execute query again");
        }

        RcFileEncoding rcFileEncoding;
        if (LazyBinaryColumnarSerDe.class.getName().equals(storage.getStorageFormat().getSerDe())) {
            rcFileEncoding = new BinaryRcFileEncoding();
        }
        else if (ColumnarSerDe.class.getName().equals(storage.getStorageFormat().getSerDe())) {
            rcFileEncoding = createTextVectorEncoding(getHiveSchema(storage.getSerdeParameters(), tableParameters), hiveStorageTimeZone);
        }
        else {
            return Optional.empty();
        }

        if (fileSize == 0) {
            throw new PrestoException(HIVE_BAD_DATA, "RCFile is empty: " + path);
        }

        FSDataInputStream inputStream;
        try {
            inputStream = hdfsEnvironment.getFileSystem(session.getUser(), path, configuration).openFile(path, hiveFileContext);
        }
        catch (Exception e) {
            if (nullToEmpty(e.getMessage()).trim().equals("Filesystem closed") ||
                    e instanceof FileNotFoundException) {
                throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, e);
            }
            throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, splitError(e, path, start, length), e);
        }

        try {
            ImmutableMap.Builder<Integer, Type> readColumns = ImmutableMap.builder();
            for (HiveColumnHandle column : columns) {
                readColumns.put(column.getHiveColumnIndex(), column.getHiveType().getType(typeManager));
            }

            RcFileReader rcFileReader = new RcFileReader(
                    new HdfsRcFileDataSource(path.toString(), inputStream, fileSize, stats),
                    rcFileEncoding,
                    readColumns.build(),
                    new AircompressorCodecFactory(new HadoopCodecFactory(configuration.getClassLoader())),
                    start,
                    length,
                    new DataSize(8, Unit.MEGABYTE));

            return Optional.of(new RcFilePageSource(rcFileReader, columns, typeManager));
        }
        catch (Throwable e) {
            try {
                inputStream.close();
            }
            catch (IOException ignored) {
            }
            if (e instanceof PrestoException) {
                throw (PrestoException) e;
            }
            String message = splitError(e, path, start, length);
            if (e instanceof RcFileCorruptionException) {
                throw new PrestoException(HIVE_BAD_DATA, message, e);
            }
            if (e.getClass().getSimpleName().equals("BlockMissingException")) {
                throw new PrestoException(HIVE_MISSING_DATA, message, e);
            }
            throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, message, e);
        }
    }

    private static String splitError(Throwable t, Path path, long start, long length)
    {
        return format("Error opening Hive split %s (offset=%s, length=%s): %s", path, start, length, t.getMessage());
    }

    public static TextRcFileEncoding createTextVectorEncoding(Properties schema, DateTimeZone hiveStorageTimeZone)
    {
        // separators
        int nestingLevels;
        if (!"true".equalsIgnoreCase(schema.getProperty(SERIALIZATION_EXTEND_NESTING_LEVELS))) {
            nestingLevels = TEXT_LEGACY_NESTING_LEVELS;
        }
        else {
            nestingLevels = TEXT_EXTENDED_NESTING_LEVELS;
        }
        byte[] separators = Arrays.copyOf(DEFAULT_SEPARATORS, nestingLevels);

        // the first three separators are set by old-old properties
        separators[0] = getByte(schema.getProperty(FIELD_DELIM, schema.getProperty(SERIALIZATION_FORMAT)), DEFAULT_SEPARATORS[0]);
        // for map field collection delimiter, Hive 1.x uses "colelction.delim" but Hive 3.x uses "collection.delim"
        // https://issues.apache.org/jira/browse/HIVE-16922
        separators[1] = getByte(schema.getProperty(COLLECTION_DELIM, schema.getProperty("colelction.delim")), DEFAULT_SEPARATORS[1]);
        separators[2] = getByte(schema.getProperty(MAPKEY_DELIM), DEFAULT_SEPARATORS[2]);

        // null sequence
        Slice nullSequence;
        String nullSequenceString = schema.getProperty(SERIALIZATION_NULL_FORMAT);
        if (nullSequenceString == null) {
            nullSequence = DEFAULT_NULL_SEQUENCE;
        }
        else {
            nullSequence = Slices.utf8Slice(nullSequenceString);
        }

        // last column takes rest
        String lastColumnTakesRestString = schema.getProperty(SERIALIZATION_LAST_COLUMN_TAKES_REST);
        boolean lastColumnTakesRest = "true".equalsIgnoreCase(lastColumnTakesRestString);

        // escaped
        String escapeProperty = schema.getProperty(ESCAPE_CHAR);
        Byte escapeByte = null;
        if (escapeProperty != null) {
            escapeByte = getByte(escapeProperty, (byte) '\\');
        }

        return new TextRcFileEncoding(
                hiveStorageTimeZone,
                nullSequence,
                separators,
                escapeByte,
                lastColumnTakesRest);
    }
}
