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
package com.facebook.presto.raptor.filesystem;

import com.facebook.presto.common.PrestoException;
import com.facebook.presto.common.security.ConnectorIdentity;
import com.facebook.presto.hive.HdfsContext;
import io.airlift.slice.XxHash64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_ERROR;

public final class FileSystemUtil
{
    public static final HdfsContext DEFAULT_RAPTOR_CONTEXT = new HdfsContext(new ConnectorIdentity("presto-raptor", Optional.empty(), Optional.empty()));

    private static final Configuration INITIAL_CONFIGURATION;

    static {
        Configuration.addDefaultResource("hdfs-default.xml");
        Configuration.addDefaultResource("hdfs-site.xml");

        // must not be transitively reloaded during the future loading of various Hadoop modules
        // all the required default resources must be declared above
        INITIAL_CONFIGURATION = new Configuration(false);
        Configuration defaultConfiguration = new Configuration();
        FileSystemUtil.copy(defaultConfiguration, FileSystemUtil.INITIAL_CONFIGURATION);
    }

    private FileSystemUtil() {}

    public static Configuration getInitialConfiguration()
    {
        return copy(INITIAL_CONFIGURATION);
    }

    public static Configuration copy(Configuration configuration)
    {
        Configuration copy = new Configuration(false);
        copy(configuration, copy);
        return copy;
    }

    public static void copy(Configuration from, Configuration to)
    {
        for (Map.Entry<String, String> entry : from) {
            to.set(entry.getKey(), entry.getValue());
        }
    }

    public static long xxhash64(FileSystem fileSystem, Path file)
    {
        try (InputStream in = fileSystem.open(file)) {
            return XxHash64.hash(in);
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_ERROR, "Failed to read file: " + file, e);
        }
    }
}
