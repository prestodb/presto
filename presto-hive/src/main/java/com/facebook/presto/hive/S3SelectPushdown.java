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

import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.type.StandardTypes;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.TextInputFormat;

import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static com.facebook.presto.hive.HiveUtil.getCompressionCodec;
import static com.facebook.presto.hive.HiveUtil.getDeserializerClassName;
import static com.facebook.presto.hive.HiveUtil.getInputFormatName;
import static com.facebook.presto.hive.metastore.MetastoreUtil.getHiveSchema;
import static java.util.Arrays.stream;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_LOCATION;

/**
 * S3SelectPushdown uses Amazon S3 Select to push down queries to Amazon S3. This allows Presto to retrieve only a
 * subset of data rather than retrieving the full S3 object thus improving Presto query performance.
 */
public class S3SelectPushdown
{
    private static final Logger LOG = Logger.get(S3SelectPushdown.class);
    private static final Set<String> SUPPORTED_S3_PREFIXES = ImmutableSet.of(
            "s3://", "s3a://", "s3n://");
    private static final Set<String> SUPPORTED_SERDES = ImmutableSet.of(
            "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
    private static final Set<String> SUPPORTED_INPUT_FORMATS = ImmutableSet.of(
            "org.apache.hadoop.mapred.TextInputFormat");
    private static final Set<String> SUPPORTED_COLUMN_TYPES = ImmutableSet.of(
            "boolean",
            "int",
            "tinyint",
            "smallint",
            "bigint",
            "string",
            "float",
            "double",
            "decimal",
            "date",
            "timestamp");

    private final HiveClientConfig hiveClientConfig;

    S3SelectPushdown(HiveClientConfig hiveClientConfig)
    {
        this.hiveClientConfig = requireNonNull(hiveClientConfig);
    }

    private static boolean isSerdeSupported(Properties schema)
    {
        String serdeName = getDeserializerClassName(schema);

        if (!SUPPORTED_SERDES.contains(serdeName)) {
            return false;
        }
        return true;
    }

    private static boolean isInputFormatSupported(Properties schema)
    {
        String inputFormat = getInputFormatName(schema);

        if (!SUPPORTED_INPUT_FORMATS.contains(inputFormat)) {
            return false;
        }
        return true;
    }

    public static boolean isCompressionCodecSupported(InputFormat inputFormat, Path path)
    {
        if (inputFormat instanceof TextInputFormat) {
            try {
                Optional<CompressionCodec> compressionCodec = getCompressionCodec((TextInputFormat) inputFormat, path);
                if (!compressionCodec.isPresent() ||
                        stream(S3SelectSupportedCompressionCodecs.values())
                                .anyMatch(codec -> codec.getCodecClass().isInstance(compressionCodec.get()))) {
                    return true;
                }
            }
            catch (IllegalAccessException | NoSuchFieldException e) {
                LOG.error(e, "Failed to find compressionCodec for inputFormat: %s", inputFormat.getClass().getName());
                return false;
            }
        }
        return false;
    }

    private static boolean areColumnTypesSupported(List<Column> columns)
    {
        if (columns == null || columns.isEmpty()) {
            return false;
        }

        for (Column column : columns) {
            String type = column.getType().getHiveTypeName().toString();
            if (type.startsWith(StandardTypes.DECIMAL)) {
                // skip precision and scale when check decimal type
                type = StandardTypes.DECIMAL;
            }
            if (!SUPPORTED_COLUMN_TYPES.contains(type.toLowerCase(Locale.ENGLISH))) {
                return false;
            }
        }

        return true;
    }

    private boolean isS3SelectEnabled(ConnectorSession session)
    {
        if (!hiveClientConfig.isS3SelectPushdownEnabled()) {
            return false;
        }

        if (!HiveSessionProperties.isS3SelectPushdownEnabled(session)) {
            return false;
        }

        return true;
    }

    private static boolean isS3Storage(Properties schema)
    {
        String location = schema.getProperty(META_TABLE_LOCATION);
        return location != null && SUPPORTED_S3_PREFIXES.stream().anyMatch(location::startsWith);
    }

    boolean shouldOptimizeTable(ConnectorSession session, Table table, Optional<Partition> partition)
    {
        if (!isS3SelectEnabled(session)) {
            return false;
        }
        // Hive table partitions could be on different storages,
        // as a result, we have to check each individual partition
        Properties schema;
        if (partition.isPresent()) {
            schema = getHiveSchema(partition.get(), table);
        }
        else {
            schema = getHiveSchema(table);
        }

        return shouldOptimizeTable(table, schema);
    }

    private boolean shouldOptimizeTable(Table table, Properties schema)
    {
        return isS3Storage(schema) &&
                isSerdeSupported(schema) &&
                isInputFormatSupported(schema) &&
                areColumnTypesSupported(table.getDataColumns());
    }

    public enum S3SelectSupportedCompressionCodecs {
        GZIP(".gz", GzipCodec.class),
        BZIP2(".bz2", BZip2Codec.class);

        private final String extension;
        private final Class<? extends CompressionCodec> codecClass;

        S3SelectSupportedCompressionCodecs(
                String extension,
                Class<? extends CompressionCodec> codecClass)
        {
            this.extension = extension;
            this.codecClass = codecClass;
        }

        public String getExtension()
        {
            return extension;
        }

        private Class<? extends CompressionCodec> getCodecClass()
        {
            return codecClass;
        }
    }
}
