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
package com.facebook.presto.hive.s3select;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HiveFileSplit;
import com.facebook.presto.hive.HiveRecordCursorProvider;
import com.facebook.presto.hive.IonSqlQueryBuilder;
import com.facebook.presto.hive.s3.PrestoS3ClientFactory;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_FILESYSTEM_ERROR;
import static com.facebook.presto.hive.HiveUtil.getDeserializerClassName;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.hive.serde.serdeConstants.COLUMN_NAME_DELIMITER;
import static org.apache.hadoop.hive.serde.serdeConstants.LIST_COLUMNS;

public class S3SelectRecordCursorProvider
        implements HiveRecordCursorProvider
{
    private final HdfsEnvironment hdfsEnvironment;
    private final HiveClientConfig clientConfig;
    private final PrestoS3ClientFactory s3ClientFactory;

    @Inject
    public S3SelectRecordCursorProvider(
            HdfsEnvironment hdfsEnvironment,
            HiveClientConfig clientConfig,
            PrestoS3ClientFactory s3ClientFactory)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.clientConfig = requireNonNull(clientConfig, "clientConfig is null");
        this.s3ClientFactory = requireNonNull(s3ClientFactory, "s3ClientFactory is null");
    }

    @Override
    public Optional<RecordCursor> createRecordCursor(
            Configuration configuration,
            ConnectorSession session,
            HiveFileSplit fileSplit,
            Properties schema,
            List<HiveColumnHandle> columns,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            DateTimeZone hiveStorageTimeZone,
            TypeManager typeManager,
            boolean s3SelectPushdownEnabled)
    {
        if (!s3SelectPushdownEnabled) {
            return Optional.empty();
        }

        Path path = new Path(fileSplit.getPath());
        try {
            this.hdfsEnvironment.getFileSystem(session.getUser(), path, configuration);
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_FILESYSTEM_ERROR, "Failed getting FileSystem: " + path, e);
        }

        // Query is not going to filter any data, no need to use S3 Select.
        if (!hasFilters(schema, effectivePredicate, columns)) {
            return Optional.empty();
        }

        String serdeName = getDeserializerClassName(schema);
        Optional<S3SelectDataType> s3SelectDataTypeOptional = S3SelectSerDeDataTypeMapper.getDataType(serdeName);

        if (s3SelectDataTypeOptional.isPresent()) {
            S3SelectDataType s3SelectDataType = s3SelectDataTypeOptional.get();

            IonSqlQueryBuilder queryBuilder = new IonSqlQueryBuilder(typeManager, s3SelectDataType);
            String ionSqlQuery = queryBuilder.buildSql(columns, effectivePredicate);
            Optional<S3SelectLineRecordReader> recordReader = S3SelectLineRecordReaderProvider.get(configuration, clientConfig, path, fileSplit.getStart(), fileSplit.getLength(), fileSplit.getFileSize(), schema, ionSqlQuery, s3ClientFactory, s3SelectDataType);

            // If S3 Select data type is not mapped to a S3SelectLineRecordReader it will return Optional.empty()
            return recordReader.map(s3SelectLineRecordReader -> new S3SelectRecordCursor<>(configuration, path, s3SelectLineRecordReader, fileSplit.getLength(), schema, columns, hiveStorageTimeZone, typeManager));
        }

        // unsupported serdes
        return Optional.empty();
    }

    private static boolean hasFilters(
            Properties schema,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            List<HiveColumnHandle> projectedColumns)
    {
        // When there are no effective predicates and the projected columns are identical to the schema, it means that
        // we get all the data out of S3. We can use S3 GetObject instead of S3 SelectObjectContent in these cases.
        if (effectivePredicate.isAll()) {
            return !areColumnsEquivalent(projectedColumns, schema);
        }
        return true;
    }

    private static boolean areColumnsEquivalent(List<HiveColumnHandle> projectedColumns, Properties schema)
    {
        Set<String> projectedColumnNames = projectedColumns.stream().map(HiveColumnHandle::getName).collect(toImmutableSet());
        Set<String> schemaColumnNames;
        String columnNameProperty = schema.getProperty(LIST_COLUMNS);
        if (columnNameProperty.length() == 0) {
            schemaColumnNames = ImmutableSet.of();
        }
        else {
            String columnNameDelimiter = (String) schema.getOrDefault(COLUMN_NAME_DELIMITER, ",");
            schemaColumnNames = ImmutableSet.copyOf(columnNameProperty.split(columnNameDelimiter));
        }
        return projectedColumnNames.equals(schemaColumnNames);
    }
}
