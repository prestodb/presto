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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import parquet.column.page.DataPage;

import static com.google.common.base.Preconditions.checkNotNull;

public class SliceColumnVector extends ColumnVector
{
    private Slice[] values;

    public SliceColumnVector()
    {
    }

    public Slice[] getValues()
    {
        checkNotNull(pages, "data pges is null");
        values = new Slice[numValues];
        int valueCount = 0;
        for (int i = 0; i < pages.length; i++) {
            DataPage page = pages[i];
            for (int j = 0; j < page.getValueCount(); j++) {
                values[valueCount] = Slices.wrappedBuffer(readers[i].readBytes().getBytes());
                valueCount = valueCount + 1;
            }
        }
        return values;
    }
}
