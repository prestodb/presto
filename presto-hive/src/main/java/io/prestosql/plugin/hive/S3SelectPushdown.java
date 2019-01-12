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
package io.prestosql.plugin.hive;

import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.prestosql.plugin.hive.metastore.Column;
import io.prestosql.plugin.hive.metastore.Partition;
import io.prestosql.plugin.hive.metastore.Table;
import io.prestosql.spi.connector.ConnectorSession;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.TextInputFormat;

import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static io.prestosql.plugin.hive.HiveSessionProperties.isS3SelectPushdownEnabled;
import static io.prestosql.plugin.hive.HiveUtil.getCompressionCodec;
import static io.prestosql.plugin.hive.HiveUtil.getDeserializerClassName;
import static io.prestosql.plugin.hive.HiveUtil.getInputFormatName;
import static io.prestosql.plugin.hive.metastore.MetastoreUtil.getHiveSchema;
import static org.apache.hadoop.hive.serde.serdeConstants.BIGINT_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.BOOLEAN_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.DATE_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.DECIMAL_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.INT_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.SMALLINT_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.STRING_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.TINYINT_TYPE_NAME;

/**
 * S3SelectPushdown uses Amazon S3 Select to push down queries to Amazon S3. This allows Presto to retrieve only a
 * subset of data rather than retrieving the full S3 object thus improving Presto query performance.
 */
public class S3SelectPushdown
{
    private static final Logger LOG = Logger.get(S3SelectPushdown.class);
    private static final Set<String> SUPPORTED_S3_PREFIXES = ImmutableSet.of("s3://", "s3a://", "s3n://");
    private static final Set<String> SUPPORTED_SERDES = ImmutableSet.of(LazySimpleSerDe.class.getName());
    private static final Set<String> SUPPORTED_INPUT_FORMATS = ImmutableSet.of(TextInputFormat.class.getName());

    /*
     * Double and Real Types lose precision. Thus, they are not pushed down to S3. Please use Decimal Type if push down is desired.
     *
     * Pushing down timestamp to s3select is problematic due to following reasons:
     * 1) Presto bug: TIMESTAMP behaviour does not match sql standard (https://github.com/prestodb/presto/issues/7122)
     * 2) Presto uses the timezone from client to convert the timestamp if no timezone is provided, however, s3select is a different service and this could lead to unexpected results.
     * 3) ION SQL compare timestamps using precision, timestamps with different precisions are not equal even actually they present the same instant of time. This could lead to unexpected results.
     */
    private static final Set<String> SUPPORTED_COLUMN_TYPES = ImmutableSet.of(
            BOOLEAN_TYPE_NAME,
            INT_TYPE_NAME,
            TINYINT_TYPE_NAME,
            SMALLINT_TYPE_NAME,
            BIGINT_TYPE_NAME,
            STRING_TYPE_NAME,
            DECIMAL_TYPE_NAME,
            DATE_TYPE_NAME);

    private S3SelectPushdown() {}

    private static boolean isSerdeSupported(Properties schema)
    {
        String serdeName = getDeserializerClassName(schema);
        return SUPPORTED_SERDES.contains(serdeName);
    }

    private static boolean isInputFormatSupported(Properties schema)
    {
        String inputFormat = getInputFormatName(schema);
        return SUPPORTED_INPUT_FORMATS.contains(inputFormat);
    }

    public static boolean isCompressionCodecSupported(InputFormat inputFormat, Path path)
    {
        if (inputFormat instanceof TextInputFormat) {
            return getCompressionCodec((TextInputFormat) inputFormat, path)
                    .map(codec -> (codec instanceof GzipCodec) || (codec instanceof BZip2Codec))
                    .orElse(true);
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
            if (column.getType().getTypeInfo() instanceof DecimalTypeInfo) {
                // skip precision and scale when check decimal type
                type = DECIMAL_TYPE_NAME;
            }
            if (!SUPPORTED_COLUMN_TYPES.contains(type)) {
                return false;
            }
        }

        return true;
    }

    private static boolean isS3Storage(String path)
    {
        return SUPPORTED_S3_PREFIXES.stream().anyMatch(path::startsWith);
    }

    static boolean shouldEnablePushdownForTable(ConnectorSession session, Table table, String path, Optional<Partition> optionalPartition)
    {
        if (!isS3SelectPushdownEnabled(session)) {
            return false;
        }

        if (path == null) {
            return false;
        }

        // Hive table partitions could be on different storages,
        // as a result, we have to check each individual optionalPartition
        Properties schema = optionalPartition
                .map(partition -> getHiveSchema(partition, table))
                .orElseGet(() -> getHiveSchema(table));
        return shouldEnablePushdownForTable(table, path, schema);
    }

    private static boolean shouldEnablePushdownForTable(Table table, String path, Properties schema)
    {
        return isS3Storage(path) &&
                isSerdeSupported(schema) &&
                isInputFormatSupported(schema) &&
                areColumnTypesSupported(table.getDataColumns());
    }
}
