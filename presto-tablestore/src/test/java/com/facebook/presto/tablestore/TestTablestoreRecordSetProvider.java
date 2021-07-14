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
package com.facebook.presto.tablestore;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.Lists;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;

import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.tablestore.TablestoreRecordSetProvider.printColumnsToGet;
import static java.util.Collections.emptyMap;
import static org.testng.Assert.assertEquals;

public class TestTablestoreRecordSetProvider
        extends TablestoreConstants
{
    @Test
    public void testGetRecordSet()
    {
        TablestoreFacade tablestoreFacade = Mockito.mock(TablestoreFacade.class);
        TablestoreRecordSetProvider provider = new TablestoreRecordSetProvider(tablestoreFacade);

        SchemaTableName stn = new SchemaTableName(SCHEMA_NAME_1, TABLE_NAME_1);
        TablestoreColumnHandle ch1 = new TablestoreColumnHandle(COLUMN_NAME_1, true, 0, VARCHAR);
        TablestoreColumnHandle ch2 = new TablestoreColumnHandle(COLUMN_NAME_2, true, 1, INTEGER);
        TablestoreColumnHandle ch3 = new TablestoreColumnHandle(COLUMN_NAME_3, false, -1, DOUBLE);
        TablestoreTableHandle th = new TablestoreTableHandle(stn, Lists.newArrayList(ch1, ch2, ch3));

        ConnectorSplit split = new TablestoreSplit(th, TupleDomain.all(), "xxx", emptyMap());
        List<? extends ColumnHandle> columns = Lists.newArrayList(ch3, ch1); //顺序故意反过来

        RecordSet rs = provider.getRecordSet(null, session(), split, columns);
        assertEquals(TablestoreRecordSet.class, rs.getClass());

        List<Type> ctypes = rs.getColumnTypes();
        assertEquals(2, ctypes.size());
        assertEquals(ch3.getColumnType(), ctypes.get(0));
        assertEquals(ch1.getColumnType(), ctypes.get(1));
    }

    @Test
    public void testPrintColumnsToGet()
    {
        TablestoreColumnHandle ch1 = new TablestoreColumnHandle(COLUMN_NAME_1, true, 0, VARCHAR);
        TablestoreColumnHandle ch2 = new TablestoreColumnHandle(COLUMN_NAME_2, true, 1, INTEGER);
        TablestoreColumnHandle ch3 = new TablestoreColumnHandle(COLUMN_NAME_3, false, -1, DOUBLE);

        List<? extends ColumnHandle> columns = Lists.newArrayList(ch1, ch2, ch3); //顺序故意反过来
        String x = printColumnsToGet(columns, 2);
        assertEquals(COLUMN_NAME_1 + ", " + COLUMN_NAME_2 + ", ..., <other 1 columns>", x);

        x = printColumnsToGet(columns, 3);
        assertEquals(COLUMN_NAME_1 + ", " + COLUMN_NAME_2 + ", " + COLUMN_NAME_3, x);

        x = printColumnsToGet(columns, 10);
        assertEquals(COLUMN_NAME_1 + ", " + COLUMN_NAME_2 + ", " + COLUMN_NAME_3, x);

        x = printColumnsToGet(Collections.emptyList(), 2);
        assertEquals("<none columns>", x);
    }
}
