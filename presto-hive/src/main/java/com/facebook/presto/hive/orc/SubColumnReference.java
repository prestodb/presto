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
package com.facebook.presto.hive.orc;

import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.spi.block.SubColumnBlock.ColumnHandleReference;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.Iterables;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

public class SubColumnReference implements ColumnHandleReference
{
    private final HiveColumnHandle reference;
    private final TypeManager typeManager;

    public SubColumnReference(TypeManager typeManager, HiveColumnHandle reference)
    {
        this.reference = reference;
        this.typeManager = typeManager;
    }

    public List<ColumnHandleReference> getHirarchies()
    {
        LinkedList<ColumnHandleReference> hierarchies = new LinkedList<>();

        HiveColumnHandle handle = reference;
        hierarchies.add(new SubColumnReference(typeManager, handle));
        // construct hierarchy
        while (handle.getParent().isPresent()) {
            handle = handle.getParent().get();
            hierarchies.push(new SubColumnReference(typeManager, handle));
        }
        return hierarchies;
    }

    public Type getType()
    {
        return typeManager.getType(reference.getTypeSignature());
    }

    public int arrayIndex()
    {
        Optional<HiveColumnHandle> parent = reference.getParent();
        // name of the field to be found
        String name = Iterables.getLast(reference.getParts());
        if (parent.isPresent()) {
            TypeInfo info = parent.get().getHiveType().getTypeInfo();
            if (info.getCategory() == ObjectInspector.Category.STRUCT) {
                StructTypeInfo structTypeInfo = (StructTypeInfo) info;
                List<String> fieldNames = structTypeInfo.getAllStructFieldNames();
                for (int i = 0; i < fieldNames.size(); i++) {
                    String field = fieldNames.get(i);
                    if (field.equals(name)) {
                        return i;
                    }
                }
            }
        }
        return 0;
    }

    public int arrayLength()
    {
        TypeInfo info = reference.getHiveType().getTypeInfo();
        if (info.getCategory() == ObjectInspector.Category.STRUCT) {
            StructTypeInfo structTypeInfo = (StructTypeInfo) info;
            return structTypeInfo.getAllStructFieldNames().size();
        }
        return 0;
    }

    public int ordinal()
    {
        return reference.getHiveColumnIndex();
    }
}
