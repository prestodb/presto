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
import com.facebook.presto.hive.cache.HiveCachingHdfsConfiguration;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordReader;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_FILESYSTEM_ERROR;
import static java.util.Objects.requireNonNull;

public class GenericHiveRecordCursorProvider
        implements HiveRecordCursorProvider
{
    private final HdfsEnvironment hdfsEnvironment;

    @Inject
    public GenericHiveRecordCursorProvider(HdfsEnvironment hdfsEnvironment)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
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
        // make sure the FileSystem is created with the proper Configuration object
        Path path = new Path(fileSplit.getPath());
        try {
            if (!fileSplit.getCustomSplitInfo().isEmpty()) {
                if (configuration instanceof HiveCachingHdfsConfiguration.CachingJobConf) {
                    configuration = ((HiveCachingHdfsConfiguration.CachingJobConf) configuration).getConfig();
                }
                if (configuration instanceof CopyOnFirstWriteConfiguration) {
                    configuration = ((CopyOnFirstWriteConfiguration) configuration).getConfig();
                }
            }
            this.hdfsEnvironment.getFileSystem(session.getUser(), path, configuration);
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_FILESYSTEM_ERROR, "Failed getting FileSystem: " + path, e);
        }

        Configuration actualConfiguration = configuration;

        RecordReader<?, ?> recordReader = hdfsEnvironment.doAs(session.getUser(),
                () -> HiveUtil.createRecordReader(actualConfiguration, path, fileSplit.getStart(), fileSplit.getLength(), schema, columns, fileSplit.getCustomSplitInfo()));
        return hdfsEnvironment.doAs(session.getUser(),
                () -> Optional.of(new GenericHiveRecordCursor<>(
                        actualConfiguration,
                        path,
                        genericRecordReader(recordReader),
                        fileSplit.getLength(),
                        schema,
                        columns,
                        hiveStorageTimeZone,
                        typeManager)));
    }

    @SuppressWarnings("unchecked")
    private static RecordReader<?, ? extends Writable> genericRecordReader(RecordReader<?, ?> recordReader)
    {
        return (RecordReader<?, ? extends Writable>) recordReader;
    }
}
