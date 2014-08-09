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

import com.facebook.presto.hive.orc.OrcReader;
import com.facebook.presto.hive.orc.OrcRecordReader;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.joda.time.DateTimeZone;

import java.util.List;
import java.util.Properties;

import static com.facebook.presto.hive.HiveUtil.getDeserializer;

public class OrcDataStreamFactory
        implements HiveDataStreamFactory
{
    @Override
    public Optional<? extends Operator> createNewDataStream(
            OperatorContext operatorContext,
            Configuration configuration,
            ConnectorSession session,
            Path path,
            long start,
            long length,
            Properties schema,
            List<HiveColumnHandle> columns,
            List<HivePartitionKey> partitionKeys,
            TupleDomain<HiveColumnHandle> tupleDomain,
            DateTimeZone hiveStorageTimeZone)
    {
        @SuppressWarnings("deprecation")
        Deserializer deserializer = getDeserializer(schema);
        if (!(deserializer instanceof OrcSerde)) {
            return Optional.absent();
        }

        OrcRecordReader recordReader;
        try {
            FileSystem fileSystem = path.getFileSystem(configuration);
            OrcReader reader = new OrcReader(path, fileSystem);
            recordReader = reader.createRecordReader(start, length, columns, tupleDomain, DateTimeZone.forID(session.getTimeZoneKey().getId()));
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }

        return Optional.of(new OrcDataStream(
                operatorContext,
                recordReader,
                partitionKeys,
                columns,
                hiveStorageTimeZone));
    }
}
