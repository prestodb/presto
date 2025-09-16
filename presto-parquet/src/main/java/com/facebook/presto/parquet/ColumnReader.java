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

import com.facebook.presto.parquet.reader.ColumnChunk;
import com.facebook.presto.parquet.reader.PageReader;
import org.apache.parquet.internal.filter2.columnindex.RowRanges;
import org.joda.time.DateTimeZone;

import java.util.Optional;

public interface ColumnReader
{
    boolean isInitialized();

    void init(PageReader pageReader, Field field, RowRanges rowRanges, Optional<DateTimeZone> timezone);

    void prepareNextRead(int batchSize);

    ColumnChunk readNext(Optional<DateTimeZone> timezone);

    long getRetainedSizeInBytes();
}
