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
package com.facebook.presto.iceberg.util;

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.common.type.StandardTypes.ARRAY;
import static com.facebook.presto.common.type.StandardTypes.MAP;
import static com.facebook.presto.common.type.StandardTypes.ROW;
import static com.google.common.base.Preconditions.checkArgument;

public class PrimitiveTypeMapBuilder
{
    private final ImmutableMap.Builder<List<String>, Type> builder = ImmutableMap.builder();

    private PrimitiveTypeMapBuilder() {}

    public static Map<List<String>, Type> makeTypeMap(List<Type> types, List<String> columnNames)
    {
        return new PrimitiveTypeMapBuilder().buildTypeMap(types, columnNames);
    }

    private Map<List<String>, Type> buildTypeMap(List<Type> types, List<String> columnNames)
    {
        for (int i = 0; i < types.size(); i++) {
            visitType(types.get(i), columnNames.get(i), ImmutableList.of());
        }
        return builder.build();
    }

    private void visitType(Type type, String name, List<String> parent)
    {
        if (ROW.equals(type.getTypeSignature().getBase())) {
            visitRowType((RowType) type, name, parent);
        }
        else if (MAP.equals(type.getTypeSignature().getBase())) {
            visitMapType((MapType) type, name, parent);
        }
        else if (ARRAY.equals(type.getTypeSignature().getBase())) {
            visitArrayType((ArrayType) type, name, parent);
        }
        else {
            builder.put(ImmutableList.<String>builder().addAll(parent).add(name).build(), type);
        }
    }

    private void visitArrayType(ArrayType type, String name, List<String> parent)
    {
        parent = ImmutableList.<String>builder().addAll(parent).add(name).add("list").build();
        visitType(type.getElementType(), "element", parent);
    }

    private void visitMapType(MapType type, String name, List<String> parent)
    {
        parent = ImmutableList.<String>builder().addAll(parent).add(name).add("map").build();
        visitType(type.getKeyType(), "key", parent);
        visitType(type.getValueType(), "value", parent);
    }

    private void visitRowType(RowType type, String name, List<String> parent)
    {
        parent = ImmutableList.<String>builder().addAll(parent).add(name).build();
        for (RowType.Field field : type.getFields()) {
            checkArgument(field.getName().isPresent(), "field in struct type doesn't have name");
            visitType(field.getType(), field.getName().get(), parent);
        }
    }
}
