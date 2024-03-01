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
package com.facebook.presto.parquet.writer;

import com.google.common.collect.Lists;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.util.LinkedList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.parquet.schema.OriginalType.LIST;
import static org.apache.parquet.schema.OriginalType.MAP;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;

// Code from iceberg
public class ParquetTypeVisitor<T>
{
    protected LinkedList<String> fieldNames = new LinkedList<>();

    public static <T> T visit(Type type, ParquetTypeVisitor<T> visitor)
    {
        if (type instanceof MessageType) {
            return visitor.message((MessageType) type, visitFields(type.asGroupType(), visitor));
        }
        else if (type.isPrimitive()) {
            return visitor.primitive(type.asPrimitiveType());
        }
        else {
            // if not a primitive, the typeId must be a group
            GroupType group = type.asGroupType();
            OriginalType annotation = group.getOriginalType();
            if (annotation == LIST) {
                checkArgument(!group.isRepetition(REPEATED),
                        "Invalid list: top-level group is repeated: " + group);
                checkArgument(group.getFieldCount() == 1,
                        "Invalid list: does not contain single repeated field: " + group);

                GroupType repeatedElement = group.getFields().get(0).asGroupType();
                checkArgument(repeatedElement.isRepetition(REPEATED),
                        "Invalid list: inner group is not repeated");
                checkArgument(repeatedElement.getFieldCount() <= 1,
                        "Invalid list: repeated group is not a single field: " + group);

                visitor.fieldNames.push(repeatedElement.getName());
                try {
                    T elementResult = null;
                    if (repeatedElement.getFieldCount() > 0) {
                        elementResult = visitField(repeatedElement.getType(0), visitor);
                    }

                    return visitor.list(group, elementResult);
                }
                finally {
                    visitor.fieldNames.pop();
                }
            }
            else if (annotation == MAP) {
                checkArgument(!group.isRepetition(REPEATED),
                        "Invalid map: top-level group is repeated: " + group);
                checkArgument(group.getFieldCount() == 1,
                        "Invalid map: does not contain single repeated field: " + group);

                GroupType repeatedKeyValue = group.getType(0).asGroupType();
                checkArgument(repeatedKeyValue.isRepetition(REPEATED),
                        "Invalid map: inner group is not repeated");
                checkArgument(repeatedKeyValue.getFieldCount() <= 2,
                        "Invalid map: repeated group does not have 2 fields");

                visitor.fieldNames.push(repeatedKeyValue.getName());
                try {
                    T keyResult = null;
                    T valueResult = null;
                    if (repeatedKeyValue.getFieldCount() == 2) {
                        keyResult = visitField(repeatedKeyValue.getType(0), visitor);
                        valueResult = visitField(repeatedKeyValue.getType(1), visitor);
                    }
                    else if (repeatedKeyValue.getFieldCount() == 1) {
                        Type keyOrValue = repeatedKeyValue.getType(0);
                        if (keyOrValue.getName().equalsIgnoreCase("key")) {
                            keyResult = visitField(keyOrValue, visitor);
                            // value result remains null
                        }
                        else {
                            valueResult = visitField(keyOrValue, visitor);
                            // key result remains null
                        }
                    }
                    return visitor.map(group, keyResult, valueResult);
                }
                finally {
                    visitor.fieldNames.pop();
                }
            }
            return visitor.struct(group, visitFields(group, visitor));
        }
    }

    private static <T> T visitField(Type field, ParquetTypeVisitor<T> visitor)
    {
        visitor.fieldNames.push(field.getName());
        try {
            return visit(field, visitor);
        }
        finally {
            visitor.fieldNames.pop();
        }
    }

    private static <T> List<T> visitFields(GroupType group, ParquetTypeVisitor<T> visitor)
    {
        List<T> results = Lists.newArrayListWithExpectedSize(group.getFieldCount());
        for (Type field : group.getFields()) {
            results.add(visitField(field, visitor));
        }

        return results;
    }

    public T message(MessageType message, List<T> fields)
    {
        return null;
    }

    public T struct(GroupType struct, List<T> fields)
    {
        return null;
    }

    public T list(GroupType array, T element)
    {
        return null;
    }

    public T map(GroupType map, T key, T value)
    {
        return null;
    }

    public T primitive(PrimitiveType primitive)
    {
        return null;
    }
}
