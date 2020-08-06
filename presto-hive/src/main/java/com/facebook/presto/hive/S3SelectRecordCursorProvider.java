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

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.s3.PrestoS3ClientFactory;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_FILESYSTEM_ERROR;
import static com.facebook.presto.hive.HiveUtil.getDeserializerClassName;
import static java.util.Objects.requireNonNull;

public class S3SelectRecordCursorProvider
        implements HiveRecordCursorProvider
{
    private static final Set<String> CSV_SERDES = ImmutableSet.of(LazySimpleSerDe.class.getName());
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
            Path path,
            long start,
            long length,
            long fileSize,
            Properties schema,
            List<HiveColumnHandle> columns,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            DateTimeZone hiveStorageTimeZone,
            TypeManager typeManager,
            boolean s3SelectPushdownEnabled,
            Map<String, String> customSplitInfo)
    {
        if (!s3SelectPushdownEnabled) {
            return Optional.empty();
        }

        try {
            this.hdfsEnvironment.getFileSystem(session.getUser(), path, configuration);
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_FILESYSTEM_ERROR, "Failed getting FileSystem: " + path, e);
        }

        String serdeName = getDeserializerClassName(schema);
        if (CSV_SERDES.contains(serdeName)) {
            IonSqlQueryBuilder queryBuilder = new IonSqlQueryBuilder(typeManager);
            String ionSqlQuery = queryBuilder.buildSql(columns, effectivePredicate);
            S3SelectLineRecordReader recordReader = new S3SelectCsvRecordReader(configuration, clientConfig, path, start, length, schema, ionSqlQuery, s3ClientFactory);
            return Optional.of(new S3SelectRecordCursor(configuration, path, recordReader, length, schema, columns, hiveStorageTimeZone, typeManager));
        }

        // unsupported serdes
        return Optional.empty();
    }
}
