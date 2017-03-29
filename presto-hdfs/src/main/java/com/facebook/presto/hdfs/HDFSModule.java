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
package com.facebook.presto.hdfs;

import com.facebook.presto.hdfs.fs.FSFactory;
import com.facebook.presto.hdfs.metaserver.JDBCMetaServer;
import com.facebook.presto.hdfs.metaserver.MetaServer;
import com.facebook.presto.spi.type.TypeManager;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

/**
 * Main binder class.
 * Bind interface to implementation class,
 * Bind class in scopes such as SINGLETON,
 * Bind class to created instance.
 *
 * @author jelly.guodong.jin@gmail.com
 */
public class HDFSModule
implements Module
{
    private final String connectorId;
    private final TypeManager typeManager;

    public HDFSModule(String connectorId, TypeManager typeManager)
    {
        this.connectorId = requireNonNull(connectorId);
        this.typeManager = requireNonNull(typeManager);
    }

    /**
     * Contributes bindings and other configurations for this module to {@code binder}.
     *
     * @param binder binder
     */
    @Override
    public void configure(Binder binder)
    {
        binder.bind(HDFSConnectorId.class).toInstance(new HDFSConnectorId(connectorId));
        binder.bind(TypeManager.class).toInstance(typeManager);

        configBinder(binder).bindConfig(HDFSConfig.class);

        binder.bind(MetaServer.class).to(JDBCMetaServer.class).in(Scopes.SINGLETON);
        binder.bind(HDFSMetadataFactory.class).in(Scopes.SINGLETON);
        binder.bind(HDFSMetadata.class).in(Scopes.SINGLETON);
        binder.bind(FSFactory.class).in(Scopes.SINGLETON);
        binder.bind(HDFSConnector.class).in(Scopes.SINGLETON);
        binder.bind(HDFSSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(HDFSPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(ClassLoader.class).toInstance(HDFSPlugin.getClassLoader());
    }
}
