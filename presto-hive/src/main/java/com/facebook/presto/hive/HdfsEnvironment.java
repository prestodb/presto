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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.inject.Inject;

import java.io.IOException;
import java.net.URI;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

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
        this.hdfsConfiguration = checkNotNull(hdfsConfiguration, "hdfsConfiguration is null");
        this.verifyChecksum = checkNotNull(config, "config is null").isVerifyChecksum();
    }

    public Configuration getConfiguration(Path path)
    {
        URI uri = path.toUri();
        if ("file".equals(uri.getScheme()) || "maprfs".equals(uri.getScheme())) {
            return new Configuration();
        }

        String host = uri.getHost();
        checkArgument(host != null, "path host is null: %s", path);
        return hdfsConfiguration.getConfiguration(host);
    }

    public FileSystem getFileSystem(Path path)
            throws IOException
    {
        FileSystem fileSystem = path.getFileSystem(getConfiguration(path));
        fileSystem.setVerifyChecksum(verifyChecksum);

        return fileSystem;
    }
}
