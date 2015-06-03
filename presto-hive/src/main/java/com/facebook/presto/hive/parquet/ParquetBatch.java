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

import com.facebook.presto.spi.type.Type;

import parquet.schema.MessageType;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class ParquetBatch
{
    private int size;
    private MessageType requestedSchema;
    private List<Type> types;
    private ColumnVector[] columns;

    public ParquetBatch(MessageType requestedSchema, List<Type> types)
    {
        this.requestedSchema = requestedSchema;
        this.types = types;
    }

    public MessageType getRequestedSchema()
    {
        return this.requestedSchema;
    }

    public int getSize()
    {
        return size;
    }

    public List<Type> getTypes()
    {
        return types;
    }

    public ColumnVector[] getColumns()
    {
        return columns;
    }

    public void setColumns(ColumnVector[] columns)
    {
        checkNotNull(columns, "columns");
        this.columns = columns;

        size = -1;
        for (ColumnVector columnVector : columns) {
            if (size == -1) {
                size = columnVector.size();
            }
            else {
                if (size != columnVector.size()) {
                    throw new RuntimeException("column vectors have different sizes");
                }
            }
        }
    }
}
