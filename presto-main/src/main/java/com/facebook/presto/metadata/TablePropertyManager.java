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
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.facebook.presto.metadata.SessionPropertyManager.evaluatePropertyValue;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
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
        Map<String, PropertyMetadata<?>> tableProperties = catalogTableProperties.get(catalog);
        if (tableProperties == null) {
            throw new PrestoException(NOT_FOUND, "Catalog not found: " + catalog);
        }

        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        for (PropertyMetadata<?> tableProperty : tableProperties.values()) {
            Expression expression = sqlPropertyValues.get(tableProperty.getName());
            Object value;
            if (expression != null) {
                Object sqlObjectValue = evaluatePropertyValue(expression, tableProperty.getSqlType(), session, metadata);
                value = tableProperty.decode(sqlObjectValue);
            }
            else {
                value = tableProperty.getDefaultValue();
            }
            // do not include default properties that are null
            if (value != null) {
                properties.put(tableProperty.getName(), value);
            }
        }
        return properties.build();
    }

    public Map<String, Map<String, PropertyMetadata<?>>> getAllTableProperties()
    {
        return ImmutableMap.copyOf(catalogTableProperties);
    }
}
