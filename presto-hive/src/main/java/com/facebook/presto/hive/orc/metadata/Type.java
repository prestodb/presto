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
package com.facebook.presto.hive.orc.metadata;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class Type
{
    public enum Kind
    {
        BOOLEAN,

        BYTE,
        SHORT,
        INT,
        LONG,
        DECIMAL,

        FLOAT,
        DOUBLE,

        STRING,
        VARCHAR,
        CHAR,

        BINARY,

        DATE,
        TIMESTAMP,

        LIST,
        MAP,
        STRUCT,
        UNION,
    }

    private final Kind kind;
    private final List<Integer> fieldTypeIndexes;
    private final List<String> fieldNames;

    public Type(Kind kind, List<Integer> fieldTypeIndexes, List<String> fieldNames)
    {
        this.kind = checkNotNull(kind, "kind is null");
        this.fieldTypeIndexes = ImmutableList.copyOf(checkNotNull(fieldTypeIndexes, "fieldTypeIndexes is null"));
        if (fieldNames == null || (fieldNames.isEmpty() && !fieldTypeIndexes.isEmpty())) {
            this.fieldNames = null;
        }
        else {
            this.fieldNames = ImmutableList.copyOf(checkNotNull(fieldNames, "fieldNames is null"));
            checkArgument(fieldNames.size() == fieldTypeIndexes.size(), "fieldNames and fieldTypeIndexes have different sizes");
        }
    }

    public Kind getKind()
    {
        return kind;
    }

    public int getFieldCount()
    {
        return fieldTypeIndexes.size();
    }

    public int getFieldTypeIndex(int field)
    {
        return fieldTypeIndexes.get(field);
    }

    public String getFieldName(int field)
    {
        return fieldNames.get(field);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("kind", kind)
                .add("fieldTypeIndexes", fieldTypeIndexes)
                .add("fieldNames", fieldNames)
                .toString();
    }
}
