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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class AbstractSessionPropertyManager
        implements SessionPropertyConfigurationManager
{
    private static final String DEFAULT_PROPERTIES = "defaultProperties";
    private static final String OVERRIDE_PROPERTIES = "overrideProperties";

    @Override
    public final SystemSessionPropertyConfiguration getSystemSessionProperties(SessionConfigurationContext context)
    {
        Map<String, String> defaultProperties = new HashMap<>();
        Set<String> overridePropertyNames = new HashSet<>();
        for (SessionMatchSpec sessionMatchSpec : getSessionMatchSpecs()) {
            Map<String, String> newProperties = sessionMatchSpec.match(sessionMatchSpec.getSessionProperties(), context);
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

        return new SystemSessionPropertyConfiguration(defaultProperties, overrideProperties);
    }

    @Override
    public final Map<String, Map<String, String>> getCatalogSessionProperties(SessionConfigurationContext context)
    {
        Map<String, Map<String, String>> catalogProperties = new HashMap<>();
        for (SessionMatchSpec sessionMatchSpec : getSessionMatchSpecs()) {
            Map<String, Map<String, String>> newProperties = sessionMatchSpec.match(sessionMatchSpec.getCatalogSessionProperties(), context);
            catalogProperties.putAll(newProperties);
        }
        return catalogProperties;
    }

    protected abstract List<SessionMatchSpec> getSessionMatchSpecs();
}
