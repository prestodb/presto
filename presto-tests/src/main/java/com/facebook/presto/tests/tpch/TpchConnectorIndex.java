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
package com.facebook.presto.tests.tpch;

import com.facebook.presto.spi.ConnectorIndex;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.IndexPageSource;
import com.facebook.presto.tests.tpch.TpchIndexedData.IndexedTable;
import com.google.common.base.Function;

import static java.util.Objects.requireNonNull;

class TpchConnectorIndex
        implements ConnectorIndex
{
    private final Function<IndexPageSource, IndexPageSource> keyFormatter;
    private final Function<IndexPageSource, IndexPageSource> outputFormatter;
    private final IndexedTable indexedTable;

    public TpchConnectorIndex(Function<IndexPageSource, IndexPageSource> keyFormatter, Function<IndexPageSource, IndexPageSource> outputFormatter, IndexedTable indexedTable)
    {
        this.keyFormatter = requireNonNull(keyFormatter, "keyFormatter is null");
        this.outputFormatter = requireNonNull(outputFormatter, "outputFormatter is null");
        this.indexedTable = requireNonNull(indexedTable, "indexedTable is null");
    }

    @Override
    public ConnectorPageSource lookup(IndexPageSource rawInputPageSource)
    {
        // convert the input record set from the column ordering in the query to
        // match the column ordering of the index
        IndexPageSource inputPageSource = keyFormatter.apply(rawInputPageSource);

        // lookup the values in the index
        IndexPageSource rawOutputPageSource = indexedTable.lookupKeys(inputPageSource);

        // convert the output record set of the index into the column ordering
        // expect by the query
        return outputFormatter.apply(rawOutputPageSource);
    }
}
