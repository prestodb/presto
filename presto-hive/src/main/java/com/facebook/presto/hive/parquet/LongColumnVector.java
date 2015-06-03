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

import parquet.column.page.DataPage;
import parquet.schema.PrimitiveType.PrimitiveTypeName;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

public class LongColumnVector extends ColumnVector
{
    private long[] values;
    private final PrimitiveTypeName parquetTypeName;

    public LongColumnVector(PrimitiveTypeName parquetTypeName)
    {
        this.parquetTypeName = parquetTypeName;
    }

    public long[] getValues()
        throws IOException
    {
        checkNotNull(pages, "data pges is null");
        values = new long[numValues];
        int valueCount = 0;
        for (int i = 0; i < pages.length; i++) {
            DataPage page = pages[i];
            for (int j = 0; j < page.getValueCount(); j++) {
                if (parquetTypeName == PrimitiveTypeName.INT64) {
                    values[valueCount] = readers[i].readLong();
                }
                else if (parquetTypeName == PrimitiveTypeName.INT32) {
                    values[valueCount] = (long) readers[i].readInteger();
                }
                else {
                    throw new IOException("Type Mismatch expecting Long but parquet file has: " + parquetTypeName);
                }
                valueCount = valueCount + 1;
            }
        }
        return values;
    }
}
