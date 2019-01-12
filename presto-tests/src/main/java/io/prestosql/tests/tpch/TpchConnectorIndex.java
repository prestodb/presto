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
package io.prestosql.tests.tpch;

import com.google.common.base.Function;
import io.prestosql.spi.connector.ConnectorIndex;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.RecordPageSource;
import io.prestosql.spi.connector.RecordSet;
import io.prestosql.tests.tpch.TpchIndexedData.IndexedTable;

import static java.util.Objects.requireNonNull;

class TpchConnectorIndex
        implements ConnectorIndex
{
    private final Function<RecordSet, RecordSet> keyFormatter;
    private final Function<RecordSet, RecordSet> outputFormatter;
    private final IndexedTable indexedTable;

    public TpchConnectorIndex(Function<RecordSet, RecordSet> keyFormatter, Function<RecordSet, RecordSet> outputFormatter, IndexedTable indexedTable)
    {
        this.keyFormatter = requireNonNull(keyFormatter, "keyFormatter is null");
        this.outputFormatter = requireNonNull(outputFormatter, "outputFormatter is null");
        this.indexedTable = requireNonNull(indexedTable, "indexedTable is null");
    }

    @Override
    public ConnectorPageSource lookup(RecordSet rawInputRecordSet)
    {
        // convert the input record set from the column ordering in the query to
        // match the column ordering of the index
        RecordSet inputRecordSet = keyFormatter.apply(rawInputRecordSet);

        // lookup the values in the index
        RecordSet rawOutputRecordSet = indexedTable.lookupKeys(inputRecordSet);

        // convert the output record set of the index into the column ordering
        // expect by the query
        return new RecordPageSource(outputFormatter.apply(rawOutputRecordSet));
    }
}
