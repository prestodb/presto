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
package com.facebook.presto.cassandra.util;

import com.facebook.presto.cassandra.CassandraColumnHandle;
import com.facebook.presto.cassandra.CassandraType;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.time.LocalDate;
import java.util.List;

import static com.facebook.presto.cassandra.util.CassandraCqlUtils.cqlValue;
import static com.facebook.presto.cassandra.util.CassandraCqlUtils.quoteStringLiteral;
import static com.facebook.presto.cassandra.util.CassandraCqlUtils.quoteStringLiteralForJson;
import static com.facebook.presto.cassandra.util.CassandraCqlUtils.toCQLCompatibleString;
import static com.facebook.presto.cassandra.util.CassandraCqlUtils.validColumnName;
import static com.facebook.presto.cassandra.util.CassandraCqlUtils.validSchemaName;
import static com.facebook.presto.cassandra.util.CassandraCqlUtils.validTableName;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Float.floatToRawIntBits;
import static org.testng.Assert.assertEquals;

public class TestCassandraCqlUtils
{
    @Test
    public void testValidSchemaName()
    {
        assertEquals("\"foo\"", validSchemaName("foo"));
        assertEquals("\"select\"", validSchemaName("select"));
    }

    @Test
    public void testValidTableName()
    {
        assertEquals("\"foo\"", validTableName("foo"));
        assertEquals("\"Foo\"", validTableName("Foo"));
        assertEquals("\"select\"", validTableName("select"));
    }

    @Test
    public void testValidColumnName()
    {
        assertEquals("\"foo\"", validColumnName("foo"));
        assertEquals("\"\"", validColumnName(CassandraCqlUtils.EMPTY_COLUMN_NAME));
        assertEquals("\"\"", validColumnName(""));
        assertEquals("\"select\"", validColumnName("select"));
    }

    @Test
    public void testQuote()
    {
        assertEquals("'foo'", quoteStringLiteral("foo"));
        assertEquals("'Presto''s'", quoteStringLiteral("Presto's"));
    }

    @Test
    public void testQuoteJson()
    {
        assertEquals("\"foo\"", quoteStringLiteralForJson("foo"));
        assertEquals("\"Presto's\"", quoteStringLiteralForJson("Presto's"));
        assertEquals("\"xx\\\"xx\"", quoteStringLiteralForJson("xx\"xx"));
    }

    @Test
    public void testCqlValueDate()
    {
        long days = LocalDate.parse("2026-09-06").toEpochDay();
        assertEquals(cqlValue(toCQLCompatibleString(days), CassandraType.DATE), "'2026-09-06'");
        assertEquals(cqlValue(toCQLCompatibleString(0L), CassandraType.DATE), "'1970-01-01'");
        assertEquals(cqlValue(toCQLCompatibleString(-1L), CassandraType.DATE), "'1969-12-31'");
    }

    @Test
    public void testCqlValueFloat()
    {
        assertEquals(cqlValue(toCQLCompatibleString((long) floatToRawIntBits(6.5f)), CassandraType.FLOAT), "6.5");
        assertEquals(cqlValue(toCQLCompatibleString((long) floatToRawIntBits(-0.25f)), CassandraType.FLOAT), "-0.25");
        assertEquals(cqlValue(toCQLCompatibleString((long) floatToRawIntBits(1.0e10f)), CassandraType.FLOAT), "1.0E10");
    }

    @Test
    public void testCqlValueInet()
    {
        assertEquals(cqlValue(toCQLCompatibleString(utf8Slice("10.0.0.6")), CassandraType.INET), "'10.0.0.6'");
        assertEquals(cqlValue(toCQLCompatibleString(utf8Slice("127.0.0.1")), CassandraType.INET), "'127.0.0.1'");
        assertEquals(cqlValue(toCQLCompatibleString(utf8Slice("::1")), CassandraType.INET), "'::1'");
    }

    @Test
    public void testCqlValueUnchangedTypes()
    {
        assertEquals(cqlValue(toCQLCompatibleString(60L), CassandraType.INT), "60");
        assertEquals(cqlValue(toCQLCompatibleString(6.5), CassandraType.DOUBLE), "6.5");
        assertEquals(cqlValue(toCQLCompatibleString(utf8Slice("o'brien")), CassandraType.TEXT), "'o''brien'");
    }

    @Test
    public void testAppendSelectColumns()
    {
        List<CassandraColumnHandle> columns = ImmutableList.of(
                new CassandraColumnHandle("", "foo", 0, CassandraType.VARCHAR, null, false, false, false, false),
                new CassandraColumnHandle("", "bar", 0, CassandraType.VARCHAR, null, false, false, false, false),
                new CassandraColumnHandle("", "table", 0, CassandraType.VARCHAR, null, false, false, false, false));

        StringBuilder sb = new StringBuilder();
        CassandraCqlUtils.appendSelectColumns(sb, columns);
        String str = sb.toString();

        assertEquals("\"foo\",\"bar\",\"table\"", str);
    }
}
