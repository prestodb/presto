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
package com.facebook.presto.hive.metastore;

import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.hive.ColumnConverter;
import com.facebook.presto.hive.HiveType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;

import java.util.Optional;

import static com.google.common.base.Strings.emptyToNull;

public class HiveColumnConverter
        implements ColumnConverter
{
    public HiveColumnConverter() {}

    @Override
    public Column toColumn(FieldSchema fieldSchema)
    {
        return new Column(fieldSchema.getName(), HiveType.valueOf(fieldSchema.getType()), Optional.ofNullable(emptyToNull(fieldSchema.getComment())), Optional.empty());
    }

    @Override
    public FieldSchema fromColumn(Column column)
    {
        return new FieldSchema(column.getName(), column.getType().getHiveTypeName().toString(), column.getComment().orElse(null));
    }

    @Override
    public TypeSignature getTypeSignature(HiveType hiveType, Optional<String> typeMetadata)
    {
        return hiveType.getTypeSignature();
    }
}
