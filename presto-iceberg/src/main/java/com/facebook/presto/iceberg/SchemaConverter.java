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
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.TypeUtil;

import static com.facebook.presto.iceberg.NestedFieldConverter.toIcebergNestedField;
import static com.facebook.presto.iceberg.NestedFieldConverter.toPrestoNestedField;
import static com.google.common.collect.ImmutableList.toImmutableList;

public final class SchemaConverter
{
    private SchemaConverter() {}

    public static PrestoIcebergSchema toPrestoSchema(Schema schema, TypeManager typeManager)
    {
        return new PrestoIcebergSchema(
                schema.schemaId(),
                schema.columns().stream()
                        .map(column -> toPrestoNestedField(column, typeManager))
                        .collect(toImmutableList()),
                ImmutableMap.copyOf(TypeUtil.indexByName(schema.asStruct())),
                schema.getAliases(),
                schema.identifierFieldIds());
    }

    public static Schema toIcebergSchema(PrestoIcebergSchema schema)
    {
        return new Schema(
                schema.getSchemaId(),
                schema.getColumns().stream()
                        .map(nestedField -> toIcebergNestedField(nestedField, schema.getColumnNameToIdMapping()))
                        .collect(toImmutableList()),
                schema.getAliases().isEmpty() ? null : schema.getAliases(),
                schema.getIdentifierFieldIds());
    }
}
