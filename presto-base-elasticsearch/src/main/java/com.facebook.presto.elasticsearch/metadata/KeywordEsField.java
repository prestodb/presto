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
import java.util.Map;
import java.util.Objects;

/**
 * SQL-related information about an index field with keyword type
 */
public class KeywordEsField
        extends EsField
{
    private final int precision;
    private final boolean normalized;

    public KeywordEsField(String name)
    {
        this(name, Collections.emptyMap(), true, DataType.KEYWORD.defaultPrecision, false);
    }

    public KeywordEsField(String name, Map<String, EsField> properties, boolean hasDocValues, int precision, boolean normalized)
    {
        super(name, DataType.KEYWORD, properties, hasDocValues);
        this.precision = precision;
        this.normalized = normalized;
    }

    @Override
    public int getPrecision()
    {
        return precision;
    }

    @Override
    public boolean isExact()
    {
        return normalized == false;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        KeywordEsField that = (KeywordEsField) o;
        return precision == that.precision &&
                normalized == that.normalized;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(super.hashCode(), precision, normalized);
    }
}
