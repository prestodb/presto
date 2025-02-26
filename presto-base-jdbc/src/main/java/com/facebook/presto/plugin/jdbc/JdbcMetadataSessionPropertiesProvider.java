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
package com.facebook.presto.plugin.jdbc;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;

import static com.facebook.presto.spi.session.PropertyMetadata.booleanProperty;

public class JdbcMetadataSessionPropertiesProvider
        implements JdbcSessionPropertiesProvider
{
    private final List<PropertyMetadata<?>> sessionProperties;
    private static final String USE_JDBC_METADATA_CACHE = "use_jdbc_metadata_cache";

    @Inject
    public JdbcMetadataSessionPropertiesProvider(JdbcMetadataConfig jdbcMetadataConfig)
    {
        this.sessionProperties = ImmutableList.of(
                booleanProperty(
                        USE_JDBC_METADATA_CACHE,
                        "Whether Jdbc Metadata In Memory Cache is Enabled",
                        jdbcMetadataConfig.isJdbcMetadataCacheEnabled(),
                        false));
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static boolean isJdbcMetadataCacheEnabled(ConnectorSession session)
    {
        try {
            return session.getProperty(USE_JDBC_METADATA_CACHE, Boolean.class);
        }
        catch (Exception e) {
            return false;
        }
    }
}
