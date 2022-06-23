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
package com.facebook.presto.connector.thrift;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;

import static com.facebook.presto.spi.session.PropertyMetadata.booleanProperty;

/**
 * Internal session properties are those defined by the connector itself.
 * These properties control certain aspects of connector's work.
 */
public final class ThriftSessionProperties
{
    private static final String SET_THRIFT_IDENTITY_HEADER = "use_thrift_identity_header";
    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public ThriftSessionProperties(ThriftConnectorConfig config)
    {
        sessionProperties = ImmutableList.of(booleanProperty(
                SET_THRIFT_IDENTITY_HEADER,
                "Thrift identity is used when set to true",
                config.getUseIdentityThriftHeader(),
                false));
    }

    public static boolean isUseIdentityThriftHeader(ConnectorSession session)
    {
        return session.getProperty(SET_THRIFT_IDENTITY_HEADER, Boolean.class);
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }
}
