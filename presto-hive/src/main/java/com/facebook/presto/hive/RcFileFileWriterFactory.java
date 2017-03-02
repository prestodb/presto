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

import com.facebook.presto.hive.metastore.StorageFormat;
import com.facebook.presto.hive.rcfile.HdfsRcFileDataSource;
import com.facebook.presto.rcfile.RcFileDataSource;
import com.facebook.presto.rcfile.RcFileEncoding;
import com.facebook.presto.rcfile.binary.BinaryRcFileEncoding;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

import static com.facebook.presto.hive.HiveType.toHiveTypes;
import static com.facebook.presto.hive.rcfile.RcFilePageSourceFactory.createTextVectorEncoding;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMNS;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMN_TYPES;

public class RcFileFileWriterFactory
        implements HiveFileWriterFactory
{
    private final DateTimeZone hiveStorageTimeZone;
    private final HdfsEnvironment hdfsEnvironment;
    private final TypeManager typeManager;
    private final NodeVersion nodeVersion;

    @Inject
    public RcFileFileWriterFactory(
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager,
            NodeVersion nodeVersion,
            HiveClientConfig hiveClientConfig)
    {
        this(hdfsEnvironment, typeManager, nodeVersion, requireNonNull(hiveClientConfig, "hiveClientConfig is null").getDateTimeZone());
    }

    public RcFileFileWriterFactory(
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager,
            NodeVersion nodeVersion,
            DateTimeZone hiveStorageTimeZone)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.nodeVersion = requireNonNull(nodeVersion, "nodeVersion is null");
        this.hiveStorageTimeZone = requireNonNull(hiveStorageTimeZone, "hiveStorageTimeZone is null");
    }

    @Override
    public Optional<HiveFileWriter> createFileWriter(
            Path path,
            List<String> inputColumnNames,
            StorageFormat storageFormat,
            Properties schema,
            JobConf configuration,
            ConnectorSession session)
    {
        if (!HiveSessionProperties.isRcfileOptimizedWriterEnabled(session)) {
            return Optional.empty();
        }

        if (!RCFileOutputFormat.class.getName().equals(storageFormat.getOutputFormat())) {
            return Optional.empty();
        }

        RcFileEncoding rcFileEncoding;
        if (LazyBinaryColumnarSerDe.class.getName().equals(storageFormat.getSerDe())) {
            rcFileEncoding = new BinaryRcFileEncoding();
        }
        else if (ColumnarSerDe.class.getName().equals(storageFormat.getSerDe())) {
            rcFileEncoding = createTextVectorEncoding(schema, hiveStorageTimeZone);
        }
        else {
            return Optional.empty();
        }

        Optional<String> codecName = Optional.ofNullable(configuration.get(FileOutputFormat.COMPRESS_CODEC));

        // existing tables and partitions may have columns in a different order than the writer is providing, so build
        // and index to rearrange columns in the proper order
        List<String> fileColumnNames = Splitter.on(',').trimResults().omitEmptyStrings().splitToList(schema.getProperty(META_TABLE_COLUMNS, ""));
        List<Type> fileColumnTypes = toHiveTypes(schema.getProperty(META_TABLE_COLUMN_TYPES, "")).stream()
                .map(hiveType -> hiveType.getType(typeManager))
                .collect(toList());

        int[] fileInputColumnIndexes = fileColumnNames.stream()
                .mapToInt(inputColumnNames::indexOf)
                .toArray();

        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(session.getUser(), path, configuration);
            OutputStream outputStream = fileSystem.create(path);

            Optional<Supplier<RcFileDataSource>> validationInputFactory = Optional.empty();
            if (HiveSessionProperties.isRcfileOptimizedWriterValidate(session)) {
                validationInputFactory = Optional.of(() -> {
                    try {
                        return new HdfsRcFileDataSource(
                                path.toString(),
                                fileSystem.open(path),
                                fileSystem.getFileStatus(path).getLen());
                    }
                    catch (IOException e) {
                        throw Throwables.propagate(e);
                    }
                });
            }

            Callable<Void> rollbackAction = () -> {
                fileSystem.delete(path, false);
                return null;
            };

            return Optional.of(new RcFileFileWriter(
                    outputStream,
                    rollbackAction,
                    rcFileEncoding,
                    fileColumnTypes,
                    codecName,
                    fileInputColumnIndexes,
                    ImmutableMap.<String, String>builder()
                            .put(HiveMetadata.PRESTO_VERSION_NAME, nodeVersion.toString())
                            .put(HiveMetadata.PRESTO_QUERY_ID_NAME, session.getQueryId())
                            .build(),
                    validationInputFactory));
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
