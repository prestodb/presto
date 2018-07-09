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

import java.util.Map;

/**
 * SQL-related information about an index field with text type
 */
public class TextEsField
        extends EsField
{
    public TextEsField(String name, Map<String, EsField> properties, boolean hasDocValues)
    {
        super(name, DataType.TEXT, properties, hasDocValues);
    }

    @Override
    public EsField getExactField()
    {
        EsField field = null;
        for (EsField property : getProperties().values()) {
            if (property.getDataType() == DataType.KEYWORD && property.isExact()) {
                if (field != null) {
                    throw new MappingException("Multiple exact keyword candidates available for [" + getName() +
                            "]; specify which one to use");
                }
                field = property;
            }
        }
        if (field == null) {
            throw new MappingException("No keyword/multi-field defined exact matches for [" + getName() +
                    "]; define one or use MATCH/QUERY instead");
        }
        return field;
    }

    @Override
    public boolean isExact()
    {
        return false;
    }
}
