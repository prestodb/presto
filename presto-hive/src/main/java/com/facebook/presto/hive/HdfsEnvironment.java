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
import com.facebook.presto.hive.authentication.GenericExceptionAction;
import com.facebook.presto.hive.authentication.HdfsAuthentication;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.security.Identity;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.inject.Inject;

import java.io.IOException;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class HdfsEnvironment
{
    static {
        HadoopNative.requireHadoopNative();
        HadoopFileSystemCache.initialize();
    }

    private final HdfsConfiguration hdfsConfiguration;
    private final HdfsAuthentication hdfsAuthentication;
    private final boolean verifyChecksum;

    @Inject
    public HdfsEnvironment(
            HdfsConfiguration hdfsConfiguration,
            HiveClientConfig config,
            HdfsAuthentication hdfsAuthentication)
    {
        this.hdfsConfiguration = requireNonNull(hdfsConfiguration, "hdfsConfiguration is null");
        this.verifyChecksum = requireNonNull(config, "config is null").isVerifyChecksum();
        this.hdfsAuthentication = requireNonNull(hdfsAuthentication, "hdfsAuthentication is null");
    }

    public Configuration getConfiguration(HdfsContext context, Path path)
    {
        return hdfsConfiguration.getConfiguration(context, path.toUri());
    }

    public FileSystem getFileSystem(HdfsContext context, Path path)
            throws IOException
    {
        return getFileSystem(context.getIdentity().getUser(), path, getConfiguration(context, path));
    }

    public FileSystem getFileSystem(String user, Path path, Configuration configuration)
            throws IOException
    {
        return hdfsAuthentication.doAs(user, () -> {
            FileSystem fileSystem = path.getFileSystem(configuration);
            fileSystem.setVerifyChecksum(verifyChecksum);
            return fileSystem;
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

    public static class HdfsContext
    {
        private final Identity identity;
        private final Optional<String> source;
        private final Optional<String> queryId;
        private final Optional<String> schemaName;
        private final Optional<String> tableName;

        public HdfsContext(Identity identity)
        {
            this.identity = requireNonNull(identity, "identity is null");
            this.source = Optional.empty();
            this.queryId = Optional.empty();
            this.schemaName = Optional.empty();
            this.tableName = Optional.empty();
        }

        public HdfsContext(ConnectorSession session, String schemaName)
        {
            requireNonNull(session, "session is null");
            requireNonNull(schemaName, "schemaName is null");
            this.identity = requireNonNull(session.getIdentity(), "session.getIdentity() is null");
            this.source = requireNonNull(session.getSource(), "session.getSource()");
            this.queryId = Optional.of(session.getQueryId());
            this.schemaName = Optional.of(schemaName);
            this.tableName = Optional.empty();
        }

        public HdfsContext(ConnectorSession session, String schemaName, String tableName)
        {
            requireNonNull(session, "session is null");
            requireNonNull(schemaName, "schemaName is null");
            requireNonNull(tableName, "tableName is null");
            this.identity = requireNonNull(session.getIdentity(), "session.getIdentity() is null");
            this.source = requireNonNull(session.getSource(), "session.getSource()");
            this.queryId = Optional.of(session.getQueryId());
            this.schemaName = Optional.of(schemaName);
            this.tableName = Optional.of(tableName);
        }

        public Identity getIdentity()
        {
            return identity;
        }

        public Optional<String> getSource()
        {
            return source;
        }

        public Optional<String> getQueryId()
        {
            return queryId;
        }

        public Optional<String> getSchemaName()
        {
            return schemaName;
        }

        public Optional<String> getTableName()
        {
            return tableName;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .omitNullValues()
                    .add("user", identity)
                    .add("source", source.orElse(null))
                    .add("queryId", queryId.orElse(null))
                    .add("schemaName", schemaName.orElse(null))
                    .add("tableName", tableName.orElse(null))
                    .toString();
        }
    }
}
