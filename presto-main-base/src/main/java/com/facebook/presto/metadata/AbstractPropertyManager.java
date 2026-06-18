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
package com.facebook.presto.metadata;

import com.facebook.presto.Session;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.planner.ParameterRewriter;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.Parameter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.facebook.presto.common.type.TypeUtils.writeNativeValue;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.sql.planner.ExpressionInterpreter.evaluateConstantExpression;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

abstract class AbstractPropertyManager
{
    private final ConcurrentMap<ConnectorId, Map<String, PropertyMetadata<?>>> connectorProperties = new ConcurrentHashMap<>();
    private final String propertyType;
    private final ErrorCodeSupplier propertyError;

    protected AbstractPropertyManager(String propertyType, ErrorCodeSupplier propertyError)
    {
        requireNonNull(propertyType, "propertyType is null");
        this.propertyType = propertyType;
        this.propertyError = requireNonNull(propertyError, "propertyError is null");
    }

    public final void addProperties(ConnectorId connectorId, List<PropertyMetadata<?>> properties)
    {
        requireNonNull(connectorId, "connectorId is null");
        requireNonNull(properties, "properties is null");

        Map<String, PropertyMetadata<?>> propertiesByName = Maps.uniqueIndex(properties, PropertyMetadata::getName);

        checkState(connectorProperties.putIfAbsent(connectorId, propertiesByName) == null, "Properties for connector '%s' are already registered", connectorId);
    }

    public final void removeProperties(ConnectorId connectorId)
    {
        connectorProperties.remove(connectorId);
    }

    public final ImmutableMap.Builder<String, Object> getUserSpecifiedProperties(
            ConnectorId connectorId,
            String catalog, // only use this for error messages
            Map<String, Expression> sqlPropertyValues,
            Session session,
            Metadata metadata,
            Map<NodeRef<Parameter>, Expression> parameters)
    {
        Map<String, PropertyMetadata<?>> supportedProperties = getSupportedProperties(connectorId, catalog);
        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();

        // Fill in user-specified properties
        for (Map.Entry<String, Expression> sqlProperty : sqlPropertyValues.entrySet()) {
            String propertyName = sqlProperty.getKey().toLowerCase(ENGLISH);
            PropertyMetadata<?> property = supportedProperties.get(propertyName);
            if (property == null) {
                throw new PrestoException(propertyError,
                        format("Catalog '%s' does not support %s property '%s'",
                                catalog,
                                propertyType,
                                propertyName));
            }

            PropertyMetadata.AdditionalSqlTypeHandler usedSqlTypeHandler = null;
            Object sqlObjectValue = null;
            try {
                sqlObjectValue = evaluatePropertyValue(sqlProperty.getValue(), property.getSqlType(), session, metadata, parameters);
            }
            catch (SemanticException e) {
                for (PropertyMetadata.AdditionalSqlTypeHandler additionalSqlTypeHandler : property.getAdditionalSqlTypeHandlers()) {
                    try {
                        sqlObjectValue = evaluatePropertyValue(sqlProperty.getValue(), additionalSqlTypeHandler.getSqlType(), session, metadata, parameters);
                        usedSqlTypeHandler = additionalSqlTypeHandler;
                        break;
                    }
                    catch (Exception ex) {
                        // ignored
                    }
                }

                if (usedSqlTypeHandler == null) {
                    String additionalTypesInfo = property.getAdditionalSqlTypeHandlers().stream()
                            .map(handler -> handler.getSqlType().getDisplayName())
                            .reduce((type, type2) -> type + ", " + type2)
                            .map(message -> " or any of [" + message + "]")
                            .orElse("");
                    throw new PrestoException(propertyError,
                            format("Invalid value for %s property '%s': Cannot convert '%s' to %s",
                                    propertyType,
                                    property.getName(),
                                    sqlProperty.getValue(),
                                    property.getSqlType()) + additionalTypesInfo, e);
                }
            }

            Object value;
            try {
                if (usedSqlTypeHandler != null) {
                    value = property.decode(sqlObjectValue, usedSqlTypeHandler.getDecoder());
                }
                else {
                    value = property.decode(sqlObjectValue);
                }
            }
            catch (Exception e) {
                throw new PrestoException(propertyError,
                        format("Unable to set %s property '%s' to '%s': %s",
                                propertyType,
                                property.getName(),
                                sqlProperty.getValue(),
                                e.getMessage()), e);
            }

            properties.put(property.getName(), value);
        }
        return properties;
    }

    public final Map<String, Object> getProperties(
            ConnectorId connectorId,
            String catalog, // only use this for error messages
            Map<String, Expression> sqlPropertyValues,
            Session session,
            Metadata metadata,
            Map<NodeRef<Parameter>, Expression> parameters)
    {
        Map<String, PropertyMetadata<?>> supportedProperties = getSupportedProperties(connectorId, catalog);
        ImmutableMap.Builder<String, Object> properties = getUserSpecifiedProperties(
                connectorId,
                catalog,
                sqlPropertyValues,
                session,
                metadata,
                parameters);
        Map<String, Object> userSpecifiedProperties = properties.build();

        // Fill in the remaining properties with non-null defaults
        for (PropertyMetadata<?> propertyMetadata : supportedProperties.values()) {
            if (!userSpecifiedProperties.containsKey(propertyMetadata.getName())) {
                Object value = propertyMetadata.getDefaultValue();
                if (value != null) {
                    properties.put(propertyMetadata.getName(), value);
                }
            }
        }
        return properties.build();
    }

    public Map<ConnectorId, Map<String, PropertyMetadata<?>>> getAllProperties()
    {
        return ImmutableMap.copyOf(connectorProperties);
    }

    private Map<String, PropertyMetadata<?>> getSupportedProperties(ConnectorId connectorId, String catalog)
    {
        Map<String, PropertyMetadata<?>> supportedProperties = connectorProperties.get(connectorId);
        if (supportedProperties == null) {
            throw new PrestoException(NOT_FOUND, "Catalog not found: " + catalog);
        }
        return supportedProperties;
    }

    private Object evaluatePropertyValue(Expression expression, Type expectedType, Session session, Metadata metadata, Map<NodeRef<Parameter>, Expression> parameters)
    {
        Expression rewritten = ExpressionTreeRewriter.rewriteWith(new ParameterRewriter(parameters), expression);
        Object value = evaluateConstantExpression(rewritten, expectedType, metadata, session, parameters);

        // convert to object value type of SQL type
        BlockBuilder blockBuilder = expectedType.createBlockBuilder(null, 1);
        writeNativeValue(expectedType, blockBuilder, value);
        Object objectValue = expectedType.getObjectValue(session.getSqlFunctionProperties(), blockBuilder, 0);

        if (objectValue == null) {
            throw new PrestoException(propertyError, format("Invalid null value for %s property", propertyType));
        }
        return objectValue;
    }
}
