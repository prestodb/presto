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
package com.facebook.presto.elasticsearch.metadata;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;

public final class Types
{
    private Types() {}

    public static Map<String, EsField> fromEs(Map<String, Object> asMap)
    {
        Map<String, Object> props = null;
        if (asMap != null && !asMap.isEmpty()) {
            props = (Map<String, Object>) asMap.get("properties");
        }
        return props == null || props.isEmpty() ? emptyMap() : startWalking(props);
    }

    private static Map<String, EsField> startWalking(Map<String, Object> mapping)
    {
        Map<String, EsField> types = new LinkedHashMap<>();

        if (mapping == null) {
            return emptyMap();
        }
        for (Map.Entry<String, Object> entry : mapping.entrySet()) {
            walkMapping(entry.getKey(), entry.getValue(), types);
        }

        return types;
    }

    private static DataType getType(Map<String, Object> content)
    {
        if (content.containsKey("type")) {
            try {
                return DataType.fromEsType(content.get("type").toString());
            }
            catch (IllegalArgumentException ex) {
                return DataType.UNSUPPORTED;
            }
        }
        else if (content.containsKey("properties")) {
            return DataType.OBJECT;
        }
        else {
            return DataType.UNSUPPORTED;
        }
    }

    @SuppressWarnings("unchecked")
    private static void walkMapping(String name, Object value, Map<String, EsField> mapping)
    {
        // object type - only root or nested docs supported
        if (value instanceof Map) {
            Map<String, Object> content = (Map<String, Object>) value;

            // extract field type
            DataType esDataType = getType(content);
            final Map<String, EsField> properties;
            if (esDataType == DataType.OBJECT || esDataType == DataType.NESTED) {
                properties = fromEs(content);
            }
            else if (content.containsKey("fields")) {
                // Check for multifields
                Object fields = content.get("fields");
                if (fields instanceof Map) {
                    properties = startWalking((Map<String, Object>) fields);
                }
                else {
                    properties = Collections.emptyMap();
                }
            }
            else {
                properties = Collections.emptyMap();
            }
            boolean docValues = boolSetting(content.get("doc_values"), esDataType.defaultDocValues);
            final EsField field;
            switch (esDataType) {
                case TEXT:
                    field = new TextEsField(name, properties, docValues);
                    break;
                case KEYWORD:
                    int length = intSetting(content.get("ignore_above"), esDataType.defaultPrecision);
                    boolean normalized = Strings.hasText(textSetting(content.get("normalizer"), null));
                    field = new KeywordEsField(name, properties, docValues, length, normalized);
                    break;
                case DATE:
                    Object fmt = content.get("format");
                    if (fmt != null) {
                        field = new DateEsField(name, properties, docValues, Strings.delimitedListToStringArray(fmt.toString(), "||"));
                    }
                    else {
                        field = new DateEsField(name, properties, docValues);
                    }
                    break;
                case UNSUPPORTED:
                    String type = content.get("type").toString();
                    field = new UnsupportedEsField(name, type);
                    break;
                default:
                    field = new EsField(name, esDataType, properties, docValues);
            }
            mapping.put(name, field);
        }
        else {
            throw new IllegalArgumentException("Unrecognized mapping " + value);
        }
    }

    private static String textSetting(Object value, String defaultValue)
    {
        return value == null ? defaultValue : value.toString();
    }

    private static boolean boolSetting(Object value, boolean defaultValue)
    {
        return value == null ? defaultValue : !(value.equals("false") || value.equals("0") || value.equals("off") || value.equals("no"));
    }

    private static int intSetting(Object value, int defaultValue)
    {
        return value == null ? defaultValue : Integer.parseInt(value.toString());
    }
}
