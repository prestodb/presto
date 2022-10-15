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
package org.apache.parquet.crypto;

import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;

public final class ParquetCryptoMetaDataUtils
{
    private ParquetCryptoMetaDataUtils()
    {
    }

    public static MessageType removeColumnsInSchema(MessageType schema, Set<ColumnPath> paths)
    {
        List<String> currentPath = new ArrayList<>();
        List<Type> prunedFields = removeColumnsInFields(schema.getFields(), currentPath, paths);
        return new MessageType(schema.getName(), prunedFields);
    }

    private static List<Type> removeColumnsInFields(List<Type> fields, List<String> currentPath, Set<ColumnPath> paths)
    {
        List<Type> prunedFields = new ArrayList<>();
        for (Type childField : fields) {
            Type prunedChildField = removeColumnsInField(childField, currentPath, paths);
            if (prunedChildField != null) {
                prunedFields.add(prunedChildField);
            }
        }
        return prunedFields;
    }

    private static Type removeColumnsInField(Type field, List<String> currentPath, Set<ColumnPath> paths)
    {
        String fieldName = field.getName();
        currentPath.add(fieldName);
        ColumnPath path = ColumnPath.get(currentPath.toArray(new String[0]));
        Type prunedField = null;
        if (!paths.contains(path)) {
            if (field.isPrimitive()) {
                prunedField = field;
            }
            else {
                List<Type> childFields = ((GroupType) field).getFields();
                List<Type> prunedFields = removeColumnsInFields(childFields, currentPath, paths);
                if (prunedFields.size() > 0) {
                    prunedField = ((GroupType) field).withNewFields(prunedFields);
                }
            }
        }

        checkState(currentPath.size() > 0,
                "The length of currentPath is empty but trying to remove element in it");
        checkState(currentPath.get(currentPath.size() - 1) != null,
                "The last element of currentPath is null");
        checkState(currentPath.get(currentPath.size() - 1).equals(fieldName),
                "The last element of currentPath " + currentPath.get(currentPath.size() - 1) + " not equal to " + fieldName);

        currentPath.remove(currentPath.size() - 1);

        return prunedField;
    }
}
