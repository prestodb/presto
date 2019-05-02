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
package com.facebook.presto.parquet;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.PrimitiveType;

import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;

// extension of parquet's ColumnDescriptor. Exposes full Primitive type information
public class RichColumnDescriptor
        extends ColumnDescriptor
{
    private final PrimitiveType primitiveType;
    private final boolean required;

    public RichColumnDescriptor(
            ColumnDescriptor descriptor,
            PrimitiveType primitiveType)
    {
        super(descriptor.getPath(), primitiveType.getPrimitiveTypeName(), primitiveType.getTypeLength(), descriptor.getMaxRepetitionLevel(), descriptor.getMaxDefinitionLevel());
        this.primitiveType = primitiveType;
        this.required = primitiveType.getRepetition() != OPTIONAL;
    }

    public PrimitiveType getPrimitiveType()
    {
        return primitiveType;
    }

    public boolean isRequired()
    {
        return required;
    }
}
