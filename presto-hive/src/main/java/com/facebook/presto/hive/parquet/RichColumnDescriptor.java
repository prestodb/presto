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
package com.facebook.presto.hive.parquet;

import parquet.column.ColumnDescriptor;
import parquet.schema.PrimitiveType;

// extension of parquet's ColumnDescriptor. Exposes full Primitive type information
public class RichColumnDescriptor
        extends ColumnDescriptor
{
    private final PrimitiveType primitiveType;

    public RichColumnDescriptor(
            String[] path,
            PrimitiveType primitiveType,
            int maxRep,
            int maxDef)
    {
        super(path, primitiveType.getPrimitiveTypeName(), primitiveType.getTypeLength(), maxRep, maxDef);
        this.primitiveType = primitiveType;
    }

    public PrimitiveType getPrimitiveType()
    {
        return primitiveType;
    }
}
