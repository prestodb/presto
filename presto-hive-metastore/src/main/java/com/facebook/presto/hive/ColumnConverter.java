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
package com.facebook.presto.hive;

import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.hive.metastore.Column;
import org.apache.hadoop.hive.metastore.api.FieldSchema;

import java.util.Optional;

public interface ColumnConverter
{
    /**
     * Converts the provided {@param fieldSchema} to Column
     */
    Column toColumn(FieldSchema fieldSchema);

    /**
     * Converts the provided {@param column} to FieldSchema
     */
    FieldSchema fromColumn(Column column);

    /**
     * Generates a new TypeSignature using {@param hiveType} and extra {@param typeMetadata}
     */
    TypeSignature getTypeSignature(HiveType hiveType, Optional<String> typeMetadata);

    /**
     * Issue #17249: clean up
     * Generates serialized typeMetadata using {@param hiveType} and @param typeSignature
     * Empty if there is no typeMetadata
     */
    Optional<String> getTypeMetadata(HiveType hiveType, TypeSignature typeSignature);
}
