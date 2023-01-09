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
package com.facebook.presto.tpcds;

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.google.common.collect.ImmutableList;
import com.teradata.tpcds.Results;
import com.teradata.tpcds.Session;
import com.teradata.tpcds.Table;
import com.teradata.tpcds.column.Column;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.CharType.createCharType;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DecimalType.createDecimalType;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.TimeType.TIME;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.teradata.tpcds.Results.constructResults;
import static com.teradata.tpcds.Table.CALL_CENTER;
import static com.teradata.tpcds.Table.DBGEN_VERSION;
import static com.teradata.tpcds.Table.getTable;
import static com.teradata.tpcds.column.CallCenterColumn.CC_CALL_CENTER_SK;
import static com.teradata.tpcds.column.CallCenterColumn.CC_MANAGER;
import static com.teradata.tpcds.column.CallCenterColumn.CC_NAME;
import static com.teradata.tpcds.column.CallCenterColumn.CC_REC_END_DATE;
import static com.teradata.tpcds.column.CallCenterColumn.CC_REC_START_DATE;
import static com.teradata.tpcds.column.CallCenterColumn.CC_SQ_FT;
import static com.teradata.tpcds.column.CallCenterColumn.CC_STATE;
import static com.teradata.tpcds.column.CallCenterColumn.CC_TAX_PERCENTAGE;
import static com.teradata.tpcds.column.DbgenVersionColumn.DV_CREATE_DATE;
import static com.teradata.tpcds.column.DbgenVersionColumn.DV_CREATE_TIME;
import static com.teradata.tpcds.column.DbgenVersionColumn.DV_VERSION;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestTpcdsRecordSet
{
    private final Session session = Session.getDefaultSession().withScale(.01);

    @Test
    public void testGetColumnTypes()
    {
        Table table = getTable(CALL_CENTER.getName());
        List<Column> columns = ImmutableList.of(
                CC_CALL_CENTER_SK,  // Big Int
                CC_SQ_FT,           // Integer
                CC_NAME,            // Varchar
                CC_STATE,           // CharType
                CC_TAX_PERCENTAGE); // Decimal
        RecordSet recordSet = new TpcdsRecordSet(constructResults(table, session), columns);
        assertEquals(recordSet.getColumnTypes(), ImmutableList.of(
                BIGINT,
                INTEGER,
                createVarcharType(50),
                createCharType(2),
                createDecimalType(5, 2)));

        table = getTable(DBGEN_VERSION.getName());
        columns = ImmutableList.of(
                DV_CREATE_DATE, // Date
                DV_CREATE_TIME, // Time
                DV_VERSION);    // Varchar
        recordSet = new TpcdsRecordSet(constructResults(table, session), columns);
        assertEquals(recordSet.getColumnTypes(), ImmutableList.of(
                DATE,
                TIME,
                createVarcharType(16)));

        table = getTable(DBGEN_VERSION.getName());
        columns = ImmutableList.of();
        recordSet = new TpcdsRecordSet(constructResults(table, session), columns);
        assertEquals(recordSet.getColumnTypes(), ImmutableList.of());
    }

    @Test
    public void testCursor()
    {
        Table table = getTable(CALL_CENTER.getName());
        Results result = constructResults(table, session);
        RecordSet recordSet = new TpcdsRecordSet(result, ImmutableList.copyOf(table.getColumns()));

        try (RecordCursor cursor = recordSet.cursor()) {
            if (cursor.advanceNextPosition()) {
                // Date
                assertFalse(cursor.isNull(CC_REC_START_DATE.ordinal()));
                assertEquals(cursor.getLong(CC_REC_START_DATE.ordinal()), 10227);
                assertTrue(cursor.isNull(CC_REC_END_DATE.ordinal()));

                // Big Int
                assertFalse(cursor.isNull(CC_CALL_CENTER_SK.ordinal()));
                assertEquals(cursor.getLong(CC_CALL_CENTER_SK.ordinal()), 1);

                // Integer
                assertFalse(cursor.isNull(CC_SQ_FT.ordinal()));
                assertEquals(cursor.getLong(CC_SQ_FT.ordinal()), 1138);

                // Varchar
                assertFalse(cursor.isNull(CC_MANAGER.ordinal()));
                assertEquals(cursor.getSlice(CC_MANAGER.ordinal()).toStringUtf8(), "Bob Belcher");

                // Character
                assertFalse(cursor.isNull(CC_STATE.ordinal()));
                assertEquals(cursor.getSlice(CC_STATE.ordinal()).toStringUtf8(), "TN");

                // Decimal
                assertFalse(cursor.isNull(CC_TAX_PERCENTAGE.ordinal()));
                assertEquals(cursor.getDouble(CC_TAX_PERCENTAGE.ordinal()), .11);
            }
        }
    }
}
