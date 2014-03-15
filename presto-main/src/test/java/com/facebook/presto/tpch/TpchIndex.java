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
package com.facebook.presto.tpch;

import com.facebook.presto.spi.Index;
import com.facebook.presto.spi.RecordSet;
import com.google.common.base.Function;

import static com.facebook.presto.tpch.TpchIndexedData.IndexedTable;
import static com.google.common.base.Preconditions.checkNotNull;

public class TpchIndex
        implements Index
{
    private final Function<RecordSet, RecordSet> keyFormatter;
    private final Function<RecordSet, RecordSet> outputFormatter;
    private final IndexedTable indexedTable;

    public TpchIndex(Function<RecordSet, RecordSet> keyFormatter, Function<RecordSet, RecordSet> outputFormatter, IndexedTable indexedTable)
    {
        this.keyFormatter = checkNotNull(keyFormatter, "keyFormatter is null");
        this.outputFormatter = checkNotNull(outputFormatter, "outputFormatter is null");
        this.indexedTable = checkNotNull(indexedTable, "indexedTable is null");
    }

    @Override
    public RecordSet lookup(RecordSet recordSet)
    {
        return outputFormatter.apply(indexedTable.lookupKeys(keyFormatter.apply(recordSet)));
    }
}
