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
package com.facebook.presto.iceberg;

import com.facebook.presto.common.type.TypeManager;
import org.apache.iceberg.types.Types.NestedField;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.iceberg.TypeConverter.toIcebergType;
import static com.facebook.presto.iceberg.TypeConverter.toPrestoType;

public final class NestedFieldConverter
{
    private NestedFieldConverter() {}

    public static PrestoIcebergNestedField toPrestoNestedField(NestedField nestedField, TypeManager typeManager)
    {
        return new PrestoIcebergNestedField(
                nestedField.isOptional(),
                nestedField.fieldId(),
                nestedField.name(),
                toPrestoType(nestedField.type(), typeManager),
                Optional.ofNullable(nestedField.doc()));
    }

    public static NestedField toIcebergNestedField(
            PrestoIcebergNestedField nestedField,
            Map<String, Integer> columnNameToIdMapping)
    {
        return NestedField.of(
                nestedField.getId(),
                nestedField.isOptional(),
                nestedField.getName(),
                toIcebergType(nestedField.getPrestoType(), nestedField.getName(), columnNameToIdMapping),
                nestedField.getDoc().orElse(null));
    }
}
