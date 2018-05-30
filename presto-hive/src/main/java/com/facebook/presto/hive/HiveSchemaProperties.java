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
package com.facebook.presto.hive;

import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.session.PropertyMetadata.stringSessionProperty;

public final class HiveSchemaProperties
{
    public static final String LOCATION_PROPERTY = "location";

    public static final List<PropertyMetadata<?>> SCHEMA_PROPERTIES = ImmutableList.of(
            stringSessionProperty(
                    LOCATION_PROPERTY,
                    "Base file system location URI",
                    null,
                    false));

    private HiveSchemaProperties() {}

    public static Optional<String> getLocation(Map<String, Object> schemaProperties)
    {
        return Optional.ofNullable((String) schemaProperties.get(LOCATION_PROPERTY));
    }
}
