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

import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HivePageSourceFactory;
import com.facebook.presto.rcfile.AircompressorCodecFactory;
import com.facebook.presto.rcfile.HadoopCodecFactory;
import com.facebook.presto.rcfile.RcFileEncoding;
import com.facebook.presto.rcfile.RcFileReader;
import com.facebook.presto.rcfile.binary.BinaryRcFileEncoding;
import com.facebook.presto.rcfile.text.TextRcFileEncoding;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static com.facebook.presto.hive.HiveSessionProperties.isRcfileOptimizedReaderEnabled;
import static com.facebook.presto.hive.HiveUtil.getDeserializerClassName;
import static com.facebook.presto.rcfile.text.TextRcFileEncoding.DEFAULT_NULL_SEQUENCE;
import static com.facebook.presto.rcfile.text.TextRcFileEncoding.DEFAULT_SEPARATORS;
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
        implements HivePageSourceFactory
{
    private static final int TEXT_LEGACY_NESTING_LEVELS = 8;
    private static final int TEXT_EXTENDED_NESTING_LEVELS = 29;

    private final TypeManager typeManager;
    private final HdfsEnvironment hdfsEnvironment;

    @Inject
    public RcFilePageSourceFactory(TypeManager typeManager, HdfsEnvironment hdfsEnvironment)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
    }

    @Override
    public Optional<? extends ConnectorPageSource> createPageSource(
            Configuration configuration,
            ConnectorSession session,
            Path path,
            long start,
            long length,
            Properties schema,
            List<HiveColumnHandle> columns,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            DateTimeZone hiveStorageTimeZone)
    {
        if (!isRcfileOptimizedReaderEnabled(session)) {
            return Optional.empty();
        }

        RcFileEncoding rcFileEncoding;
        String deserializerClassName = getDeserializerClassName(schema);
        if (deserializerClassName.equals(LazyBinaryColumnarSerDe.class.getName())) {
            rcFileEncoding = new BinaryRcFileEncoding();
        }
        else if (deserializerClassName.equals(ColumnarSerDe.class.getName())) {
            rcFileEncoding = createTextVectorEncoding(schema, hiveStorageTimeZone);
        }
        else {
            return Optional.empty();
        }

        long size;
        FSDataInputStream inputStream;
        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(session.getUser(), path, configuration);
            size = fileSystem.getFileStatus(path).getLen();
            inputStream = fileSystem.open(path);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }

        try {
            ImmutableMap.Builder<Integer, Type> readColumns = ImmutableMap.builder();
            for (HiveColumnHandle column : columns) {
                readColumns.put(column.getHiveColumnIndex(), column.getHiveType().getType(typeManager));
            }

            RcFileReader rcFileReader = new RcFileReader(
                    new HdfsRcFileDataSource(path.toString(), inputStream, size),
                    rcFileEncoding,
                    readColumns.build(),
                    new AircompressorCodecFactory(new HadoopCodecFactory(configuration.getClassLoader())),
                    start,
                    length,
                    new DataSize(1, Unit.MEGABYTE));

            return Optional.of(new RcFilePageSource(
                    rcFileReader,
                    columns,
                    hiveStorageTimeZone,
                    typeManager
            ));
        }
        catch (Throwable e) {
            try {
                inputStream.close();
            }
            catch (IOException ignored) {
            }
            throw Throwables.propagate(e);
        }
    }

    private static TextRcFileEncoding createTextVectorEncoding(Properties schema, DateTimeZone hiveStorageTimeZone)
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
        separators[1] = getByte(schema.getProperty(COLLECTION_DELIM), DEFAULT_SEPARATORS[1]);
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
