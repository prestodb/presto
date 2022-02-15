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
package com.facebook.presto.plugin.postgresql;

import com.facebook.presto.plugin.jdbc.JdbcSessionPropertiesProvider;
import com.facebook.presto.plugin.postgresql.PostgreSqlConfig.ArrayMapping;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;

import static com.facebook.presto.spi.session.PropertyMetadata.enumProperty;

public final class PostgreSqlSessionProperties
        implements JdbcSessionPropertiesProvider
{
    private static final String ARRAY_MAPPING = "array_mapping";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public PostgreSqlSessionProperties(PostgreSqlConfig postgreSqlConfig)
    {
        sessionProperties = ImmutableList.of(
                enumProperty(ARRAY_MAPPING, "Handling of postgresql arrays", ArrayMapping.class,
                        postgreSqlConfig.getArrayMapping(), false));
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static ArrayMapping getArrayMapping(ConnectorSession session)
    {
        return session.getProperty(ARRAY_MAPPING, ArrayMapping.class);
    }
}
