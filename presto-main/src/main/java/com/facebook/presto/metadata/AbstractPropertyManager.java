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
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.planner.ParameterRewriter;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.spi.type.TypeUtils.writeNativeValue;
import static com.facebook.presto.sql.planner.ExpressionInterpreter.evaluateConstantExpression;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

abstract class AbstractPropertyManager
{
    private final ConcurrentMap<String, Map<String, PropertyMetadata<?>>> catalogProperties = new ConcurrentHashMap<>();
    private final String propertyType;
    private final ErrorCodeSupplier propertyError;

    protected AbstractPropertyManager(String propertyType, ErrorCodeSupplier propertyError)
    {
        requireNonNull(propertyType, "propertyType is null");
        this.propertyType = propertyType;
        this.propertyError = requireNonNull(propertyError, "propertyError is null");
    }

    public final void addProperties(String catalog, List<PropertyMetadata<?>> properties)
    {
        requireNonNull(catalog, "catalog is null");
        checkArgument(!catalog.isEmpty() && catalog.trim().equals(catalog), "Invalid catalog name '%s'", catalog);
        requireNonNull(properties, "properties is null");

        Map<String, PropertyMetadata<?>> propertiesByName = Maps.uniqueIndex(properties, PropertyMetadata::getName);

        checkState(catalogProperties.putIfAbsent(catalog, propertiesByName) == null, "Properties for catalog '%s' are already registered", catalog);
    }

    public final Map<String, Object> getProperties(String catalog, Map<String, Expression> sqlPropertyValues, Session session, Metadata metadata, List<Expression> parameters)
    {
        Map<String, PropertyMetadata<?>> supportedProperties = catalogProperties.get(catalog);
        if (supportedProperties == null) {
            throw new PrestoException(NOT_FOUND, "Catalog not found: " + catalog);
        }

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

            Object sqlObjectValue;
            try {
                sqlObjectValue = evaluatePropertyValue(sqlProperty.getValue(), property.getSqlType(), session, metadata, parameters);
            }
            catch (SemanticException e) {
                throw new PrestoException(propertyError,
                        format("Invalid value for %s property '%s': Cannot convert '%s' to %s",
                                propertyType,
                                property.getName(),
                                sqlProperty.getValue(),
                                property.getSqlType()), e);
            }

            Object value;
            try {
                value = property.decode(sqlObjectValue);
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

    public Map<String, Map<String, PropertyMetadata<?>>> getAllProperties()
    {
        return ImmutableMap.copyOf(catalogProperties);
    }

    private Object evaluatePropertyValue(Expression expression, Type expectedType, Session session, Metadata metadata, List<Expression> parameters)
    {
        Expression rewritten = ExpressionTreeRewriter.rewriteWith(new ParameterRewriter(parameters), expression);
        Object value = evaluateConstantExpression(rewritten, expectedType, metadata, session, parameters);

        // convert to object value type of SQL type
        BlockBuilder blockBuilder = expectedType.createBlockBuilder(new BlockBuilderStatus(), 1);
        writeNativeValue(expectedType, blockBuilder, value);
        Object objectValue = expectedType.getObjectValue(session.toConnectorSession(), blockBuilder, 0);

        if (objectValue == null) {
            throw new PrestoException(propertyError, format("Invalid null value for %s property", propertyType));
        }
        return objectValue;
    }
}
