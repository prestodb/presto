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

import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.s3.PrestoS3ClientFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.util.Optional;
import java.util.Properties;

public class S3SelectLineRecordReaderProvider
{
    private S3SelectLineRecordReaderProvider() {}

    public static Optional<S3SelectLineRecordReader> get(
            Configuration configuration,
            HiveClientConfig clientConfig,
            Path path,
            long start,
            long length,
            Properties schema,
            String ionSqlQuery,
            PrestoS3ClientFactory s3ClientFactory,
            S3SelectDataType dataType)
    {
        switch (dataType) {
            case CSV:
                return Optional.of(new S3SelectCsvRecordReader(configuration, clientConfig, path, start, length, schema, ionSqlQuery, s3ClientFactory));
            case JSON:
                return Optional.of(new S3SelectJsonRecordReader(configuration, clientConfig, path, start, length, schema, ionSqlQuery, s3ClientFactory));
            default:
                // return empty if data type is not returned by the serDeMapper or unrecognizable by the LineRecordReader
                return Optional.empty();
        }
    }
}
