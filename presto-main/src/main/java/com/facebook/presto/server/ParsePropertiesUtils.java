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
package com.facebook.presto.server;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;

public final class ParsePropertiesUtils
{
    private static final Splitter DOT_SPLITTER = Splitter.on('.');

    private ParsePropertiesUtils() {}

    public static SystemAndCatalogProperties parseSessionProperties(Map<String, String> properties, String errorMessage)
    {
        requireNonNull(properties, "properties is null");
        ImmutableMap.Builder<String, String> systemProperties = ImmutableMap.builder();
        Map<String, Map<String, String>> catalogSessionProperties = new HashMap<>();
        for (Entry<String, String> entry : properties.entrySet()) {
            String fullPropertyName = entry.getKey();
            String propertyValue = entry.getValue();
            List<String> nameParts = DOT_SPLITTER.splitToList(fullPropertyName);
            if (nameParts.size() == 1) {
                String propertyName = nameParts.get(0);

                assertRequest(!propertyName.isEmpty(), errorMessage);

                // catalog session properties can not be validated until the transaction has stated, so we delay system property validation also
                systemProperties.put(propertyName, propertyValue);
            }
            else if (nameParts.size() == 2) {
                String catalogName = nameParts.get(0);
                String propertyName = nameParts.get(1);

                assertRequest(!catalogName.isEmpty(), errorMessage);
                assertRequest(!propertyName.isEmpty(), errorMessage);

                // catalog session properties can not be validated until the transaction has stated
                catalogSessionProperties.computeIfAbsent(catalogName, id -> new HashMap<>()).put(propertyName, propertyValue);
            }
            else {
                throw badRequest(errorMessage);
            }
        }
        return new SystemAndCatalogProperties(
                systemProperties.build(),
                catalogSessionProperties.entrySet().stream()
                        .collect(toImmutableMap(Entry::getKey, entry -> ImmutableMap.copyOf(entry.getValue()))));
    }

    public static WebApplicationException badRequest(String message)
    {
        throw new WebApplicationException(Response
                .status(BAD_REQUEST)
                .type(TEXT_PLAIN)
                .entity(message)
                .build());
    }

    public static void assertRequest(boolean expression, String format, Object... args)
    {
        if (!expression) {
            throw badRequest(format(format, args));
        }
    }

    public static final class SystemAndCatalogProperties
    {
        private final Map<String, String> systemProperties;
        private final Map<String, Map<String, String>> catalogProperties;

        private SystemAndCatalogProperties(Map<String, String> systemProperties, Map<String, Map<String, String>> catalogProperties)
        {
            this.systemProperties = systemProperties;
            this.catalogProperties = catalogProperties;
        }

        public Map<String, String> getSystemProperties()
        {
            return systemProperties;
        }

        public Map<String, Map<String, String>> getCatalogProperties()
        {
            return catalogProperties;
        }
    }
}
