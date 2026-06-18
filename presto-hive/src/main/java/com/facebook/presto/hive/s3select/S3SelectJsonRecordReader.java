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

import com.amazonaws.services.s3.model.InputSerialization;
import com.amazonaws.services.s3.model.JSONInput;
import com.amazonaws.services.s3.model.JSONOutput;
import com.amazonaws.services.s3.model.JSONType;
import com.amazonaws.services.s3.model.OutputSerialization;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.s3.PrestoS3ClientFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.util.Properties;

public class S3SelectJsonRecordReader
        extends S3SelectLineRecordReader
{
    public S3SelectJsonRecordReader(Configuration configuration,
            HiveClientConfig clientConfig,
            Path path,
            long start,
            long length,
            long fileSize,
            Properties schema,
            String ionSqlQuery,
            PrestoS3ClientFactory s3ClientFactory)
    {
        super(configuration, clientConfig, path, start, length, fileSize, schema, ionSqlQuery, s3ClientFactory);
    }

    @Override
    protected InputSerialization buildInputSerialization()
    {
        // JSONType.LINES is the only JSON format supported by the Hive JsonSerDe.
        JSONInput selectObjectJSONInputSerialization = new JSONInput();
        selectObjectJSONInputSerialization.setType(JSONType.LINES);

        InputSerialization selectObjectInputSerialization = new InputSerialization();
        selectObjectInputSerialization.setCompressionType(getCompressionType());
        selectObjectInputSerialization.setJson(selectObjectJSONInputSerialization);

        return selectObjectInputSerialization;
    }

    @Override
    protected OutputSerialization buildOutputSerialization()
    {
        OutputSerialization selectObjectOutputSerialization = new OutputSerialization();
        JSONOutput selectObjectJSONOutputSerialization = new JSONOutput();
        selectObjectOutputSerialization.setJson(selectObjectJSONOutputSerialization);

        return selectObjectOutputSerialization;
    }
}
