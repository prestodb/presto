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
import java.util.Objects;

/**
 * SQL-related information about an index field that cannot be supported by SQL
 */
public class UnsupportedEsField
        extends EsField
{
    private String originalType;

    public UnsupportedEsField(String name, String originalType)
    {
        super(name, DataType.UNSUPPORTED, Collections.emptyMap(), false);
        this.originalType = originalType;
    }

    public String getOriginalType()
    {
        return originalType;
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
        UnsupportedEsField that = (UnsupportedEsField) o;
        return Objects.equals(originalType, that.originalType);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(super.hashCode(), originalType);
    }
}
