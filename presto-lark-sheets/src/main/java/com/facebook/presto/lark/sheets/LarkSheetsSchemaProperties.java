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

package com.facebook.presto.lark.sheets;

import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.session.PropertyMetadata.booleanProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.stringProperty;

public class LarkSheetsSchemaProperties
{
    private static final String PROPERTY_TOKEN = "token";
    private static final String PROPERTY_PUBLIC = "public";

    private final List<PropertyMetadata<?>> properties;

    public LarkSheetsSchemaProperties()
    {
        properties = ImmutableList.of(
                stringProperty(PROPERTY_TOKEN,
                        "Spreadsheet token for this schema",
                        null,
                        false),
                booleanProperty(
                        PROPERTY_PUBLIC,
                        "Schema is public visible",
                        false,
                        false));
    }

    public List<PropertyMetadata<?>> getSchemaProperties()
    {
        return properties;
    }

    public static Optional<String> getSchemaToken(Map<String, Object> schemaProperties)
    {
        return Optional.ofNullable((String) schemaProperties.get(PROPERTY_TOKEN));
    }

    public static boolean isSchemaPublic(Map<String, Object> schemaProperties)
    {
        return (boolean) schemaProperties.get(PROPERTY_PUBLIC);
    }
}
