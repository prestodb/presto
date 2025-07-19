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
package com.facebook.presto.session;

import com.facebook.presto.spi.session.SessionConfigurationContext;
import com.facebook.presto.spi.session.SessionPropertyConfigurationManager;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Table;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableTable.toImmutableTable;

public abstract class AbstractSessionPropertyManager
        implements SessionPropertyConfigurationManager
{
    public static final String DEFAULT_PROPERTIES = "defaultProperties";
    public static final String OVERRIDE_PROPERTIES = "overrideProperties";

    @Override
    public final SystemSessionPropertyConfiguration getSystemSessionProperties(SessionConfigurationContext context)
    {
        Map<String, Map<String, String>> sessionProperties = getSessionProperties(context);
        Map<String, String> defaultProperties = sessionProperties.get(DEFAULT_PROPERTIES);
        Map<String, String> overrideProperties = sessionProperties.get(OVERRIDE_PROPERTIES);

        return new SystemSessionPropertyConfiguration(ImmutableMap.copyOf(defaultProperties.entrySet().stream()
                .filter(property -> !isCatalogSessionProperty(property))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue))), ImmutableMap.copyOf(overrideProperties.entrySet().stream()
                .filter(property -> !isCatalogSessionProperty(property))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue))));
    }

    @Override
    public final Map<String, Map<String, String>> getCatalogSessionProperties(SessionConfigurationContext context)
    {
        Map<String, Map<String, String>> sessionProperties = getSessionProperties(context);
        Table<String, String, String> catalogsSessionProperties = sessionProperties.get(DEFAULT_PROPERTIES).entrySet().stream()
                .filter(AbstractSessionPropertyManager::isCatalogSessionProperty)
                .collect(toImmutableTable(
                        catalogProperty -> catalogProperty.getKey().split("\\.", 2)[0],
                        catalogProperty -> catalogProperty.getKey().split("\\.", 2)[1],
                        Map.Entry::getValue));
        return catalogsSessionProperties.rowMap();
    }

    protected abstract List<SessionMatchSpec> getSessionMatchSpecs();

    private Map<String, Map<String, String>> getSessionProperties(SessionConfigurationContext context)
    {
        // later properties override earlier properties
        Map<String, String> defaultProperties = new HashMap<>();
        Set<String> overridePropertyNames = new HashSet<String>();
        for (SessionMatchSpec sessionMatchSpec : getSessionMatchSpecs()) {
            Map<String, String> newProperties = sessionMatchSpec.match(context);
            defaultProperties.putAll(newProperties);
            if (sessionMatchSpec.getOverrideSessionProperties().orElse(false)) {
                overridePropertyNames.addAll(newProperties.keySet());
            }
        }

        // Once a property has been overridden it stays that way and the value is updated by any rule
        Map<String, String> overrideProperties = new HashMap<>();
        for (String propertyName : overridePropertyNames) {
            overrideProperties.put(propertyName, defaultProperties.get(propertyName));
        }

        Map<String, Map<String, String>> sessionProperties = new HashMap<>();
        sessionProperties.put(DEFAULT_PROPERTIES, defaultProperties);
        sessionProperties.put(OVERRIDE_PROPERTIES, overrideProperties);

        return sessionProperties;
    }

    private static boolean isCatalogSessionProperty(Map.Entry<String, String> property)
    {
        return property.getKey().contains(".");
    }
}
