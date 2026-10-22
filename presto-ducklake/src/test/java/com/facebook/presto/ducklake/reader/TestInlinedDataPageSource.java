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
package com.facebook.presto.ducklake.reader;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.ducklake.DuckLakeColumnHandle;
import com.facebook.presto.ducklake.DuckLakeColumnIdentity;
import com.facebook.presto.ducklake.catalog.DuckLakeInlinedRowSource;
import com.facebook.presto.hive.BaseHiveColumnHandle.ColumnType;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DecimalType.createDecimalType;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.ducklake.DuckLakeColumnHandle.PATH_COLUMN_HANDLE;
import static com.facebook.presto.ducklake.DuckLakeColumnHandle.ROW_ID_COLUMN_HANDLE;
import static com.facebook.presto.ducklake.DuckLakeColumnHandle.primitiveColumnHandle;
import static com.facebook.presto.ducklake.DuckLakeColumnIdentity.TypeCategory.PRIMITIVE;
import static com.facebook.presto.ducklake.DuckLakeColumnIdentity.TypeCategory.STRUCT;
import static com.facebook.presto.ducklake.DuckLakeErrorCode.DUCKLAKE_UNSUPPORTED_FEATURE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

/**
 * Pure in-memory unit tests: exercises {@link InlinedDataPageSource}'s value conversion against a
 * fake {@link DuckLakeInlinedRowSource}, without touching the JDBC catalog.
 */
public class TestInlinedDataPageSource
{
    private static final DuckLakeColumnHandle ID_COLUMN = primitiveColumnHandle(1, "id", "int32", INTEGER);
    private static final DuckLakeColumnHandle VAL_COLUMN = primitiveColumnHandle(2, "val", "varchar", VARCHAR);
    private static final DuckLakeColumnHandle NULL_COLUMN = primitiveColumnHandle(3, "opt", "int32", INTEGER);
    private static final DuckLakeColumnHandle DATE_COLUMN = primitiveColumnHandle(4, "d", "date", DATE);
    private static final DuckLakeColumnHandle TIMESTAMP_COLUMN = primitiveColumnHandle(5, "ts", "timestamp", TIMESTAMP);
    private static final DuckLakeColumnHandle DECIMAL_COLUMN = primitiveColumnHandle(6, "amount", "decimal(10,2)", createDecimalType(10, 2));
    private static final DuckLakeColumnHandle BOOLEAN_COLUMN = primitiveColumnHandle(7, "flag", "boolean", BOOLEAN);

    @Test
    public void testConvertsEveryColumnAndMetadataChannel()
            throws IOException
    {
        List<DuckLakeColumnHandle> columns = ImmutableList.of(
                ID_COLUMN,
                VAL_COLUMN,
                NULL_COLUMN,
                DATE_COLUMN,
                TIMESTAMP_COLUMN,
                DECIMAL_COLUMN,
                BOOLEAN_COLUMN,
                ROW_ID_COLUMN_HANDLE,
                PATH_COLUMN_HANDLE);

        FakeInlinedRowSource rowSource = new FakeInlinedRowSource(
                ImmutableList.of(100L),
                ImmutableList.of(new Object[] {
                        1,
                        "abc".getBytes(UTF_8),
                        null,
                        "2024-06-15",
                        "2024-06-15 13:45:30.123456",
                        new BigDecimal("12.34"),
                        Boolean.TRUE}));

        InlinedDataPageSource pageSource = new InlinedDataPageSource(rowSource, columns, 1024);
        Page page = pageSource.getNextPage();
        assertTrue(pageSource.isFinished());
        assertNull(pageSource.getNextPage());

        assertEquals(page.getPositionCount(), 1);
        assertEquals((int) INTEGER.getLong(page.getBlock(0), 0), 1);
        assertEquals(VARCHAR.getSlice(page.getBlock(1), 0).toStringUtf8(), "abc");
        assertTrue(isNull(page.getBlock(2)));
        assertEquals(DATE.getLong(page.getBlock(3), 0), LocalDate.parse("2024-06-15").toEpochDay());
        assertEquals(TIMESTAMP.getLong(page.getBlock(4), 0), LocalDateTime.parse("2024-06-15T13:45:30.123456").toInstant(ZoneOffset.UTC).toEpochMilli());
        assertEquals(readDecimal(page.getBlock(5)), new BigDecimal("12.34"));
        assertTrue(BOOLEAN.getBoolean(page.getBlock(6), 0));
        assertEquals(BIGINT.getLong(page.getBlock(7), 0), 100L);
        assertTrue(isNull(page.getBlock(8)));

        pageSource.close();
        assertTrue(rowSource.isClosed());
    }

    @Test
    public void testStructColumnIsRejected()
    {
        DuckLakeColumnIdentity child = new DuckLakeColumnIdentity(11, "child", PRIMITIVE, "int32", ImmutableList.of());
        DuckLakeColumnIdentity structIdentity = new DuckLakeColumnIdentity(10, "nested", STRUCT, "struct", ImmutableList.of(child));
        DuckLakeColumnHandle structColumn = new DuckLakeColumnHandle(structIdentity, INTEGER, ColumnType.REGULAR, Optional.empty());

        FakeInlinedRowSource rowSource = new FakeInlinedRowSource(ImmutableList.of(), ImmutableList.of());
        try {
            new InlinedDataPageSource(rowSource, ImmutableList.of(structColumn), 1024);
            throw new AssertionError("expected PrestoException");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), DUCKLAKE_UNSUPPORTED_FEATURE.toErrorCode());
        }
    }

    @Test
    public void testRegularColumnNamesSkipsMetadataColumns()
    {
        List<String> names = InlinedDataPageSource.regularColumnNames(
                ImmutableList.of(ID_COLUMN, ROW_ID_COLUMN_HANDLE, VAL_COLUMN, PATH_COLUMN_HANDLE));
        assertEquals(names, ImmutableList.of("id", "val"));
    }

    private static boolean isNull(Block block)
    {
        return block.isNull(0);
    }

    private static BigDecimal readDecimal(Block block)
    {
        long unscaled = BIGINT.getLong(block, 0);
        return new BigDecimal(unscaled).movePointLeft(2);
    }

    private static class FakeInlinedRowSource
            implements DuckLakeInlinedRowSource
    {
        private final List<Long> rowIds;
        private final List<Object[]> rows;
        private int index = -1;
        private boolean closed;

        FakeInlinedRowSource(List<Long> rowIds, List<Object[]> rows)
        {
            this.rowIds = rowIds;
            this.rows = rows;
        }

        @Override
        public boolean advanceNextRow()
        {
            index++;
            return index < rows.size();
        }

        @Override
        public Object getObject(int columnIndex)
        {
            return rows.get(index)[columnIndex];
        }

        @Override
        public long getRowId()
        {
            return rowIds.get(index);
        }

        @Override
        public void close()
        {
            closed = true;
        }

        boolean isClosed()
        {
            return closed;
        }
    }
}
