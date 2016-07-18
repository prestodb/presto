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
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.spi.type.TypeUtils.writeNativeValue;
import static com.facebook.presto.sql.planner.ExpressionInterpreter.evaluateConstantExpression;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class TablePropertyManager
{
    private final ConcurrentMap<String, Map<String, PropertyMetadata<?>>> catalogTableProperties = new ConcurrentHashMap<>();

    public void addTableProperties(String catalog, List<PropertyMetadata<?>> tableProperties)
    {
        requireNonNull(catalog, "catalog is null");
        checkArgument(!catalog.isEmpty() && catalog.trim().equals(catalog), "Invalid catalog name '%s'", catalog);
        requireNonNull(tableProperties, "tableProperties is null");

        Map<String, PropertyMetadata<?>> propertiesByName = Maps.uniqueIndex(tableProperties, PropertyMetadata::getName);

        checkState(catalogTableProperties.putIfAbsent(catalog, propertiesByName) == null, "TableProperties for catalog '%s' are already registered", catalog);
    }

    public Map<String, Object> getTableProperties(
            String catalog,
            Map<String, Expression> sqlPropertyValues,
            Session session,
            Metadata metadata)
    {
        Map<String, PropertyMetadata<?>> supportedTableProperties = catalogTableProperties.get(catalog);
        if (supportedTableProperties == null) {
            throw new PrestoException(NOT_FOUND, "Catalog not found: " + catalog);
        }

        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();

        // Fill in user-specified properties
        for (Map.Entry<String, Expression> sqlProperty : sqlPropertyValues.entrySet()) {
            String property = sqlProperty.getKey().toLowerCase(ENGLISH);
            PropertyMetadata<?> tableProperty = supportedTableProperties.get(property);
            if (tableProperty == null) {
                throw new PrestoException(INVALID_TABLE_PROPERTY, format("Catalog '%s' does not support table property '%s'", catalog, property));
            }

            Object sqlObjectValue;
            try {
                sqlObjectValue = evaluatePropertyValue(sqlProperty.getValue(), tableProperty.getSqlType(), session, metadata);
            }
            catch (SemanticException e) {
                throw new PrestoException(INVALID_TABLE_PROPERTY,
                        format("Invalid value for table property '%s': Cannot convert '%s' to %s",
                                tableProperty.getName(),
                                sqlProperty.getValue(),
                                tableProperty.getSqlType()), e);
            }

            Object value;
            try {
                value = tableProperty.decode(sqlObjectValue);
            }
            catch (Exception e) {
                throw new PrestoException(INVALID_TABLE_PROPERTY,
                        format("Unable to set table property '%s' to '%s': %s", tableProperty.getName(), sqlProperty.getValue(), e.getMessage()), e);
            }

            properties.put(tableProperty.getName(), value);
        }
        Map<String, Object> userSpecifiedProperties = properties.build();

        // Fill in the remaining properties with non-null defaults
        for (PropertyMetadata<?> tableProperty : supportedTableProperties.values()) {
            if (!userSpecifiedProperties.containsKey(tableProperty.getName())) {
                Object value = tableProperty.getDefaultValue();
                if (value != null) {
                    properties.put(tableProperty.getName(), value);
                }
            }
        }
        return properties.build();
    }

    public Map<String, Map<String, PropertyMetadata<?>>> getAllTableProperties()
    {
        return ImmutableMap.copyOf(catalogTableProperties);
    }

    private static Object evaluatePropertyValue(Expression expression, Type expectedType, Session session, Metadata metadata)
    {
        Object value = evaluateConstantExpression(expression, expectedType, metadata, session);

        // convert to object value type of SQL type
        BlockBuilder blockBuilder = expectedType.createBlockBuilder(new BlockBuilderStatus(), 1);
        writeNativeValue(expectedType, blockBuilder, value);
        Object objectValue = expectedType.getObjectValue(session.toConnectorSession(), blockBuilder, 0);

        if (objectValue == null) {
            throw new PrestoException(INVALID_TABLE_PROPERTY, "Table property value cannot be null");
        }
        return objectValue;
    }
}
