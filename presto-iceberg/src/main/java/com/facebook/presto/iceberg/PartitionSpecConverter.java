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
import org.apache.iceberg.PartitionSpec;

import static com.facebook.presto.iceberg.PartitionFields.parsePartitionFields;
import static com.facebook.presto.iceberg.PartitionFields.toPartitionFields;
import static com.facebook.presto.iceberg.SchemaConverter.toIcebergSchema;
import static com.facebook.presto.iceberg.SchemaConverter.toPrestoSchema;

public class PartitionSpecConverter
{
    private PartitionSpecConverter() {}

    public static PrestoIcebergPartitionSpec toPrestoPartitionSpec(PartitionSpec spec, TypeManager typeManager)
    {
        return new PrestoIcebergPartitionSpec(
                spec.specId(),
                toPrestoSchema(spec.schema(), typeManager),
                toPartitionFields(spec));
    }

    public static PartitionSpec toIcebergPartitionSpec(PrestoIcebergPartitionSpec spec)
    {
        return parsePartitionFields(
                toIcebergSchema(spec.getSchema()),
                spec.getFields(),
                spec.getSpecId());
    }
}
