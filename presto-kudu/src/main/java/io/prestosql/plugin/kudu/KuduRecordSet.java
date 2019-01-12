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
package io.prestosql.plugin.kudu;

import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.connector.RecordSet;
import io.prestosql.spi.type.Type;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduTable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class KuduRecordSet
        implements RecordSet
{
    private final KuduClientSession clientSession;
    private final KuduSplit kuduSplit;
    private final List<? extends ColumnHandle> columns;
    private final boolean containsVirtualRowId;

    public KuduRecordSet(KuduClientSession clientSession, KuduSplit kuduSplit, List<? extends ColumnHandle> columns)
    {
        this.clientSession = clientSession;
        this.kuduSplit = kuduSplit;
        this.columns = columns;
        this.containsVirtualRowId = columns.contains(KuduColumnHandle.ROW_ID_HANDLE);
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columns.stream()
                .map(column -> ((KuduColumnHandle) column).getType())
                .collect(toImmutableList());
    }

    @Override
    public RecordCursor cursor()
    {
        KuduScanner scanner = clientSession.createScanner(kuduSplit);
        if (!containsVirtualRowId) {
            return new KuduRecordCursor(scanner, getColumnTypes());
        }
        else {
            final int primaryKeyColumnCount = kuduSplit.getPrimaryKeyColumnCount();

            Map<Integer, Integer> fieldMapping = new HashMap<>();
            int index = primaryKeyColumnCount;
            for (int i = 0; i < columns.size(); i++) {
                KuduColumnHandle handle = (KuduColumnHandle) columns.get(i);
                if (!handle.isVirtualRowId()) {
                    if (handle.getOrdinalPosition() < primaryKeyColumnCount) {
                        fieldMapping.put(i, handle.getOrdinalPosition());
                    }
                    else {
                        fieldMapping.put(i, index);
                        index++;
                    }
                }
                else {
                    fieldMapping.put(i, -1);
                }
            }

            KuduTable table = getTable();
            return new KuduRecordCursorWithVirtualRowId(scanner, table, getColumnTypes(), fieldMapping);
        }
    }

    KuduTable getTable()
    {
        return kuduSplit.getTableHandle().getTable(clientSession);
    }

    KuduClientSession getClientSession()
    {
        return clientSession;
    }
}
