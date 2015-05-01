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

import com.facebook.presto.hadoop.HadoopFileSystemCache;
import com.facebook.presto.hadoop.HadoopNative;
import com.facebook.presto.spi.ConnectorSession;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.inject.Inject;

import java.io.IOException;

import static com.facebook.presto.hive.HiveSessionProperties.getAwsIamRole;
import static java.util.Objects.requireNonNull;

public class HdfsEnvironment
{
    static {
        HadoopNative.requireHadoopNative();
        HadoopFileSystemCache.initialize();
    }

    private final HdfsConfiguration hdfsConfiguration;
    private final boolean verifyChecksum;

    @Inject
    public HdfsEnvironment(HdfsConfiguration hdfsConfiguration, HiveClientConfig config)
    {
        this.hdfsConfiguration = requireNonNull(hdfsConfiguration, "hdfsConfiguration is null");
        this.verifyChecksum = requireNonNull(config, "config is null").isVerifyChecksum();
    }

    public Configuration getConfiguration(Path path, ConnectorSession session)
    {
        Configuration config = hdfsConfiguration.getConfiguration(path.toUri());
        String awsIamRole = getAwsIamRole(session);
        if (awsIamRole != null) {
            config.set(PrestoS3FileSystem.S3_ROLE_ARN, awsIamRole);
        }
        else {
            // unset s3 role since configuration object is thread local
            config.unset(PrestoS3FileSystem.S3_ROLE_ARN);
        }
        config.setBoolean("fs.s3.impl.disable.cache", true);
        config.setBoolean("fs.s3n.impl.disable.cache", true);
        return config;
    }

    public FileSystem getFileSystem(Path path, ConnectorSession session)
            throws IOException
    {
        FileSystem fileSystem = path.getFileSystem(getConfiguration(path, session));
        fileSystem.setVerifyChecksum(verifyChecksum);

        return fileSystem;
    }
}
