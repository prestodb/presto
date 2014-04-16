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
package com.facebook.presto.connector.jdbcSchema;

import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.split.ConnectorDataStreamProvider;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static com.google.inject.multibindings.Multibinder.newSetBinder;

public class JdbcSchemaModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
/*
        binder.bind(JdbcSchemaDataStreamProvider.class).in(Scopes.SINGLETON);
        binder.bind(JdbcSchemaMetadata.class).in(Scopes.SINGLETON);
        binder.bind(JdbcSchemaSplitManager.class).in(Scopes.SINGLETON);

        newMapBinder(binder, String.class, Connector.class).addBinding("jdbc").to(JdbcSchemaConnector.class);
*/

        newSetBinder(binder, ConnectorSplitManager.class).addBinding().to(JdbcSchemaSplitManager.class).in(Scopes.SINGLETON);
        newSetBinder(binder, ConnectorDataStreamProvider.class).addBinding().to(JdbcSchemaDataStreamProvider.class).in(Scopes.SINGLETON);
        newMapBinder(binder, String.class, ConnectorHandleResolver.class).addBinding("jdbc_schema").to(JdbcSchemaHandleResolver.class).in(Scopes.SINGLETON);
    }
}
