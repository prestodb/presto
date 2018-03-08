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

import com.facebook.presto.connector.thrift.api.PrestoThriftBlock;
import com.facebook.presto.connector.thrift.api.PrestoThriftSession;
import com.facebook.presto.connector.thrift.api.PrestoThriftSessionProperty;
import com.facebook.presto.connector.thrift.api.datatypes.PrestoThriftBigint;
import com.facebook.presto.connector.thrift.api.datatypes.PrestoThriftBoolean;
import com.facebook.presto.connector.thrift.api.datatypes.PrestoThriftDouble;
import com.facebook.presto.connector.thrift.api.datatypes.PrestoThriftInteger;
import com.facebook.presto.connector.thrift.api.datatypes.PrestoThriftVarchar;
import com.facebook.presto.connector.thrift.clientproviders.PrestoThriftServiceProvider;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.security.Principal;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * Internal session properties are those defined by the connector itself.
 * These properties control certain aspects of connector's work.
 */
public final class ThriftSessionProperties
{
    private final PrestoThriftServiceProvider clientProvider;
    private static final String SERVICE_SESSION_PROPERTY_PREFIX = "service";

    @Inject
    public ThriftSessionProperties(PrestoThriftServiceProvider clientProvider)
    {
        this.clientProvider = requireNonNull(clientProvider, "clientProvider is null");
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        List<PrestoThriftSessionProperty> sessionProperties = clientProvider.anyHostClient().listSessionProperties();
        if (sessionProperties == null) {
            return ImmutableList.of();
        }
        return sessionProperties.stream().map(PrestoThriftSessionProperty::getSessionProperty)
                .collect(toImmutableList());
    }

    public PrestoThriftSession convertConnectorSession(ConnectorSession session)
    {
        ImmutableMap.Builder<String, PrestoThriftBlock> properties = ImmutableMap.builder();
        for (PropertyMetadata<?> property : getSessionProperties()) {
            if (!property.getName().startsWith(PrestoThriftSessionProperty.PREFIX)) {
                continue;
            }

            PrestoThriftBlock valueBlock = getValueBlock(property, session);
            if (valueBlock != null) {
                properties.put(property.getName().substring(PrestoThriftSessionProperty.PREFIX.length()), valueBlock);
            }
        }

        return new PrestoThriftSession(session.getQueryId(),
                session.getSource().orElse(null),
                session.getUser(),
                session.getIdentity().getPrincipal().map(Principal::getName).orElse(null),
                session.getTimeZoneKey().getId(),
                session.getLocale().toString(),
                session.getStartTime(),
                properties.build());
    }

    private static <T> PrestoThriftBlock getValueBlock(PropertyMetadata<T> propertyMetadata, ConnectorSession session)
    {
        Class<?> javaType = propertyMetadata.getJavaType();
        if (javaType == Boolean.class) {
            Boolean value = session.getProperty(propertyMetadata.getName(), Boolean.class);
            if (value == null || value.equals(propertyMetadata.getDefaultValue())) {
                return null;
            }
            return PrestoThriftBlock.booleanData(new PrestoThriftBoolean(null, new boolean[] {value}));
        }
        else if (javaType == Integer.class) {
            Integer value = session.getProperty(propertyMetadata.getName(), Integer.class);
            if (value == null || value.equals(propertyMetadata.getDefaultValue())) {
                return null;
            }
            return PrestoThriftBlock.integerData(new PrestoThriftInteger(null, new int[] {value}));
        }
        else if (javaType == Long.class) {
            Long value = session.getProperty(propertyMetadata.getName(), Long.class);
            if (value == null || value.equals(propertyMetadata.getDefaultValue())) {
                return null;
            }
            return PrestoThriftBlock.bigintData(new PrestoThriftBigint(null, new long[] {value}));
        }
        else if (javaType == Double.class) {
            Double value = session.getProperty(propertyMetadata.getName(), Double.class);
            if (value == null || value.equals(propertyMetadata.getDefaultValue())) {
                return null;
            }
            return PrestoThriftBlock.doubleData(new PrestoThriftDouble(null, new double[] {value}));
        }
        else if (javaType == String.class) {
            String value = session.getProperty(propertyMetadata.getName(), String.class);
            if (value == null || value.equals(propertyMetadata.getDefaultValue())) {
                return null;
            }
            return PrestoThriftBlock.varcharData(new PrestoThriftVarchar(null, new int[] {value.length()}, value.getBytes()));
        }

        return null;
    }
}
