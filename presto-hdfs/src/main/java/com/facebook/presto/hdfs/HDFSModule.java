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

import com.facebook.presto.hdfs.metaserver.JDBCMetaServer;
import com.facebook.presto.hdfs.metaserver.MetaServer;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

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

    public HDFSModule(String connectorId)
    {
        this.connectorId = connectorId;
    }

    /**
     * Contributes bindings and other configurations for this module to {@code binder}.
     * <p>
     * <p><strong>Do not invoke this method directly</strong> to install submodules. Instead use
     * {@link Binder#install(Module)}, which ensures that {@link Provides provider methods} are
     * discovered.
     *
     * @param binder
     */
    @Override
    public void configure(Binder binder)
    {
        binder.bind(HDFSConnectorId.class).toInstance(new HDFSConnectorId(connectorId));

        binder.bind(HDFSMetadataFactory.class).in(Scopes.SINGLETON);
        // bind MetaServer to JDBC implementation, and set to SINGLETON
        binder.bind(MetaServer.class).to(JDBCMetaServer.class).in(Scopes.SINGLETON);
        binder.bind(HDFSSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(HDFSPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(HDFSPageSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(HDFSTransactionManager.class).in(Scopes.SINGLETON);
        binder.bind(HDFSConnector.class).in(Scopes.SINGLETON);
    }
}
