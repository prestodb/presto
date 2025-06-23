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

public class JdbcSessionProperties
        implements JdbcSessionPropertiesProvider
{
    private static final String PARTIAL_PREDICATE_PUSH_DOWN = "partial_predicate_push_down";
    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public JdbcSessionProperties(BaseJdbcConfig jdbcConfig)
    {
        sessionProperties = ImmutableList.of(
                booleanProperty(
                        PARTIAL_PREDICATE_PUSH_DOWN,
                        "Push down predicates partially to underlying database",
                        jdbcConfig.isPartialPredicatePushDown(),
                        false));
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static boolean isPartialPredicatePushDownEnabled(ConnectorSession session)
    {
        return session.getProperty(PARTIAL_PREDICATE_PUSH_DOWN, Boolean.class);
    }
}
