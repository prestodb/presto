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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.spi.type.Type;
import org.apache.kudu.Schema;
import org.apache.kudu.client.KeyEncoderAccessor;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;

import java.util.List;
import java.util.Map;

public class KuduRecordCursorWithVirtualRowId
        extends KuduRecordCursor
{
    private final KuduTable table;
    private final Map<Integer, Integer> fieldMapping;

    public KuduRecordCursorWithVirtualRowId(KuduScanner scanner, KuduTable table,
            List<Type> columnTypes,
            Map<Integer, Integer> fieldMapping)
    {
        super(scanner, columnTypes);
        this.table = table;
        this.fieldMapping = fieldMapping;
    }

    @Override
    protected int mapping(int field)
    {
        return fieldMapping.get(field);
    }

    @Override
    public Slice getSlice(int field)
    {
        if (fieldMapping.get(field) == -1) {
            PartialRow partialRow = buildPrimaryKey();
            return Slices.wrappedBuffer(KeyEncoderAccessor.encodePrimaryKey(partialRow));
        }
        else {
            return super.getSlice(field);
        }
    }

    private PartialRow buildPrimaryKey()
    {
        Schema schema = table.getSchema();
        PartialRow row = new PartialRow(schema);
        RowHelper.copyPrimaryKey(schema, currentRow, row);
        return row;
    }
}
