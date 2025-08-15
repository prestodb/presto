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

import com.facebook.presto.hadoop.HadoopNative;
import com.facebook.presto.hive.authentication.GenericExceptionAction;
import com.facebook.presto.hive.authentication.HdfsAuthentication;
import com.facebook.presto.hive.filesystem.ExtendedFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.HadoopExtendedFileSystemCache;
import org.apache.hadoop.fs.Path;

import javax.inject.Inject;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class HdfsEnvironment
{
    static {
        HadoopExtendedFileSystemCache.initialize();
    }

    private final HdfsConfiguration hdfsConfiguration;
    private final HdfsAuthentication hdfsAuthentication;
    private final boolean verifyChecksum;

    @Inject
    public HdfsEnvironment(
            @ForMetastoreHdfsEnvironment HdfsConfiguration hdfsConfiguration,
            MetastoreClientConfig config,
            HdfsAuthentication hdfsAuthentication)
    {
        this.hdfsConfiguration = requireNonNull(hdfsConfiguration, "hdfsConfiguration is null");
        this.verifyChecksum = requireNonNull(config, "config is null").isVerifyChecksum();
        this.hdfsAuthentication = requireNonNull(hdfsAuthentication, "hdfsAuthentication is null");
        if (config.isRequireHadoopNative()) {
            HadoopNative.requireHadoopNative();
        }
    }

    public Configuration getConfiguration(HdfsContext context, Path path)
    {
        return hdfsConfiguration.getConfiguration(context, path.toUri());
    }

    public ExtendedFileSystem getFileSystem(HdfsContext context, Path path)
            throws IOException
    {
        return getFileSystem(context.getIdentity().getUser(), path, getConfiguration(context, path));
    }

    public ExtendedFileSystem getFileSystem(String user, Path path, Configuration configuration)
            throws IOException
    {
        return hdfsAuthentication.doAs(user, () -> {
            FileSystem fileSystem = path.getFileSystem(configuration);
            fileSystem.setVerifyChecksum(verifyChecksum);
            checkState(fileSystem instanceof ExtendedFileSystem);
            return (ExtendedFileSystem) fileSystem;
        });
    }

    public <R, E extends Exception> R doAs(String user, GenericExceptionAction<R, E> action)
            throws E
    {
        return hdfsAuthentication.doAs(user, action);
    }

    public void doAs(String user, Runnable action)
    {
        hdfsAuthentication.doAs(user, action);
    }
}
