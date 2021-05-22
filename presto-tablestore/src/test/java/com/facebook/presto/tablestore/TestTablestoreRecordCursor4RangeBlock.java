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

import com.alicloud.openservices.tablestore.core.protocol.PlainBufferCrc8;
import com.alicloud.openservices.tablestore.model.ColumnType;
import com.alicloud.openservices.tablestore.model.Row;
import com.alicloud.openservices.tablestore.model.SimpleRowMatrixBlockConstants;
import com.alicloud.openservices.tablestore.model.SimpleRowMatrixBlockParser;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestTablestoreRecordCursor4RangeBlock
        extends TablestoreConstants
{
    private static byte calcCheckSum(ByteBuffer buffer)
    {
        int current = buffer.position();
        buffer.position(0);
        byte crc = 0;
        for (int i = 0; i < current; i++) {
            crc = PlainBufferCrc8.crc8(crc, buffer.get());
        }
        buffer.position(current);
        return crc;
    }

    private static void putLong(ByteBuffer buffer, long v)
    {
        buffer.put((byte) 0);
        buffer.putLong(v);
    }

    private static void putDouble(ByteBuffer buffer, double v)
    {
        buffer.put((byte) 1);
        buffer.putDouble(v);
    }

    private static void putBoolean(ByteBuffer buffer, boolean v)
    {
        buffer.put((byte) 2);
        buffer.put(v ? (byte) 1 : (byte) 0);
    }

    private static void putString(ByteBuffer buffer, String v)
    {
        buffer.put((byte) 3);
        buffer.putInt(v.length());
        buffer.put(v.getBytes());
    }

    private static void putNull(ByteBuffer buffer)
    {
        buffer.put((byte) 6);
    }

    private static void putBlob(ByteBuffer buffer, byte[] v)
    {
        buffer.put((byte) 7);
        buffer.putInt(v.length);
        buffer.put(v);
    }

    @Test
    public void testParseEmptyBlockOnlyPK()
    {
        ByteBuffer buffer = ByteBuffer.allocate(10000);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(SimpleRowMatrixBlockConstants.API_VERSION);
        buffer.putInt(0); // placeholder for data_offset
        buffer.putInt(0); // placeholder for options_offset
        buffer.putInt(2); // pk_count
        buffer.putInt(0); // attr_count
        buffer.putShort((short) 3); // pk0_length
        buffer.put("pk0".getBytes()); // pk0_name
        buffer.putShort((short) 3); // pk1_length
        buffer.put("pk1".getBytes()); // pk1_name
        buffer.putInt(8, buffer.position()); // fill options_offset
        buffer.put(SimpleRowMatrixBlockConstants.TAG_ENTIRE_PRIMARY_KEYS);
        buffer.put((byte) 1); // has entire pk
        buffer.put(SimpleRowMatrixBlockConstants.TAG_ROW_COUNT);
        buffer.putInt(0); // has 0 row
        buffer.putInt(4, buffer.position()); // fill data_offset
        buffer.put(SimpleRowMatrixBlockConstants.TAG_CHECKSUM);
        byte crc = calcCheckSum(buffer);
        buffer.put(crc);

        byte[] bytes = new byte[buffer.position()];
        buffer.position(0);
        buffer.get(bytes);

        SimpleRowMatrixBlockParser parser = new SimpleRowMatrixBlockParser(ByteBuffer.wrap(bytes));
        String[] expectFieldNames = new String[] {"pk0", "pk1"};
        String[] fieldNames = parser.parseFieldNames();
        assertEquals(expectFieldNames.length, fieldNames.length);
        for (int i = 0; i < fieldNames.length; i++) {
            assertEquals(expectFieldNames[i], fieldNames[i]);
        }
        assertEquals(2, parser.getPkCount());
        assertEquals(0, parser.getAttrCount());
        assertEquals(2, parser.getFieldCount());
        assertEquals(true, parser.hasEntirePrimaryKeys());
        assertEquals(0, parser.getRowCount());
        assertEquals(false, parser.hasNext());
        assertEquals(buffer.position(), parser.getTotalBytes());
        assertEquals(0, parser.getDataBytes());

        List<Row> rows = parser.getRows();
        assertEquals(0, rows.size());
    }

    @Test
    public void testParseEmptyBlockOnlyAttr()
    {
        ByteBuffer buffer = ByteBuffer.allocate(10000);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(SimpleRowMatrixBlockConstants.API_VERSION);
        buffer.putInt(0); // placeholder for data_offset
        buffer.putInt(0); // placeholder for options_offset
        buffer.putInt(0); // pk_count
        buffer.putInt(2); // attr_count
        buffer.putShort((short) 4); // col0_length
        buffer.put("col0".getBytes()); // col0_name
        buffer.putShort((short) 4); // col1_length
        buffer.put("col1".getBytes()); // col1_name
        buffer.putInt(8, buffer.position()); // fill options_offset
        buffer.put(SimpleRowMatrixBlockConstants.TAG_ENTIRE_PRIMARY_KEYS);
        buffer.put((byte) 0); // has entire pk
        buffer.put(SimpleRowMatrixBlockConstants.TAG_ROW_COUNT);
        buffer.putInt(0); // has 0 row
        buffer.putInt(4, buffer.position()); // fill data_offset
        buffer.put(SimpleRowMatrixBlockConstants.TAG_CHECKSUM);
        byte crc = calcCheckSum(buffer);
        buffer.put(crc);

        byte[] bytes = new byte[buffer.position()];
        buffer.position(0);
        buffer.get(bytes);

        SimpleRowMatrixBlockParser parser = new SimpleRowMatrixBlockParser(ByteBuffer.wrap(bytes));
        String[] expectFieldNames = new String[] {"col0", "col1"};
        String[] fieldNames = parser.parseFieldNames();
        assertEquals(expectFieldNames.length, fieldNames.length);
        for (int i = 0; i < fieldNames.length; i++) {
            assertEquals(expectFieldNames[i], fieldNames[i]);
        }
        assertEquals(0, parser.getPkCount());
        assertEquals(2, parser.getAttrCount());
        assertEquals(2, parser.getFieldCount());
        assertEquals(false, parser.hasEntirePrimaryKeys());
        assertEquals(0, parser.getRowCount());
        assertEquals(false, parser.hasNext());
        assertEquals(buffer.position(), parser.getTotalBytes());
        assertEquals(0, parser.getDataBytes());

        List<Row> rows = parser.getRows();
        assertEquals(0, rows.size());
    }

    @Test
    public void testParseEmptyBlockBothPKAttr()
    {
        ByteBuffer buffer = ByteBuffer.allocate(10000);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(SimpleRowMatrixBlockConstants.API_VERSION);
        buffer.putInt(0); // placeholder for data_offset
        buffer.putInt(0); // placeholder for options_offset
        buffer.putInt(2); // pk_count
        buffer.putInt(2); // attr_count
        buffer.putShort((short) 3); // pk0_length
        buffer.put("pk0".getBytes()); // pk0_name
        buffer.putShort((short) 3); // pk1_length
        buffer.put("pk1".getBytes()); // pk1_name
        buffer.putShort((short) 4); // col0_length
        buffer.put("col0".getBytes()); // col0_name
        buffer.putShort((short) 4); // col1_length
        buffer.put("col1".getBytes()); // col1_name
        buffer.putInt(8, buffer.position()); // fill options_offset
        buffer.put(SimpleRowMatrixBlockConstants.TAG_ENTIRE_PRIMARY_KEYS);
        buffer.put((byte) 0); // has entire pk
        buffer.put(SimpleRowMatrixBlockConstants.TAG_ROW_COUNT);
        buffer.putInt(0); // has 0 row
        buffer.putInt(4, buffer.position()); // fill data_offset
        buffer.put(SimpleRowMatrixBlockConstants.TAG_CHECKSUM);
        byte crc = calcCheckSum(buffer);
        buffer.put(crc);

        byte[] bytes = new byte[buffer.position()];
        buffer.position(0);
        buffer.get(bytes);

        SimpleRowMatrixBlockParser parser = new SimpleRowMatrixBlockParser(ByteBuffer.wrap(bytes));
        String[] expectFieldNames = new String[] {"pk0", "pk1", "col0", "col1"};
        String[] fieldNames = parser.parseFieldNames();
        assertEquals(expectFieldNames.length, fieldNames.length);
        for (int i = 0; i < fieldNames.length; i++) {
            assertEquals(expectFieldNames[i], fieldNames[i]);
        }
        assertEquals(2, parser.getPkCount());
        assertEquals(2, parser.getAttrCount());
        assertEquals(4, parser.getFieldCount());
        assertEquals(false, parser.hasEntirePrimaryKeys());
        assertEquals(0, parser.getRowCount());
        assertEquals(false, parser.hasNext());
        assertEquals(buffer.position(), parser.getTotalBytes());
        assertEquals(0, parser.getDataBytes());

        List<Row> rows = parser.getRows();
        assertEquals(0, rows.size());
    }

    @Test
    public void testParseOneRow()
    {
        ByteBuffer buffer = ByteBuffer.allocate(10000);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(SimpleRowMatrixBlockConstants.API_VERSION);
        buffer.putInt(0); // placeholder for data_offset
        buffer.putInt(0); // placeholder for options_offset
        buffer.putInt(2); // pk_count
        buffer.putInt(2); // attr_count
        buffer.putShort((short) 3); // pk0_length
        buffer.put("pk0".getBytes()); // pk0_name
        buffer.putShort((short) 3); // pk1_length
        buffer.put("pk1".getBytes()); // pk1_name
        buffer.putShort((short) 4); // col0_length
        buffer.put("col0".getBytes()); // col0_name
        buffer.putShort((short) 4); // col1_length
        buffer.put("col1".getBytes()); // col1_name
        buffer.putInt(8, buffer.position()); // fill options_offset
        buffer.put(SimpleRowMatrixBlockConstants.TAG_ENTIRE_PRIMARY_KEYS);
        buffer.put((byte) 0); // has entire pk
        buffer.put(SimpleRowMatrixBlockConstants.TAG_ROW_COUNT);
        buffer.putInt(1); // has 0 row
        buffer.putInt(4, buffer.position()); // fill data_offset

        buffer.put(SimpleRowMatrixBlockConstants.TAG_ROW);
        putString(buffer, "user0");
        putLong(buffer, 100L);
        putNull(buffer);
        putBoolean(buffer, true);

        buffer.put(SimpleRowMatrixBlockConstants.TAG_CHECKSUM);
        byte crc = calcCheckSum(buffer);
        buffer.put(crc);

        byte[] bytes = new byte[buffer.position()];
        buffer.position(0);
        buffer.get(bytes);

        SimpleRowMatrixBlockParser parser = new SimpleRowMatrixBlockParser(ByteBuffer.wrap(bytes));
        String[] expectFieldNames = new String[] {"pk0", "pk1", "col0", "col1"};
        String[] fieldNames = parser.parseFieldNames();
        assertEquals(expectFieldNames.length, fieldNames.length);
        for (int i = 0; i < fieldNames.length; i++) {
            assertEquals(expectFieldNames[i], fieldNames[i]);
        }
        assertEquals(2, parser.getPkCount());
        assertEquals(2, parser.getAttrCount());
        assertEquals(4, parser.getFieldCount());
        assertEquals(false, parser.hasEntirePrimaryKeys());
        assertEquals(1, parser.getRowCount());
        assertEquals(true, parser.hasNext());
        assertEquals(buffer.position(), parser.getTotalBytes());
        assertTrue(parser.getDataBytes() > 0);

        parser.next();
        assertEquals(ColumnType.STRING, parser.getColumnType(0));
        assertEquals("user0", parser.getString(0));
        assertEquals("user0", parser.getObject(0));

        assertEquals(ColumnType.INTEGER, parser.getColumnType(1));
        assertEquals(100L, parser.getLong(1));
        assertEquals(100L, parser.getObject(1));

        assertEquals(null, parser.getColumnType(2));
        assertEquals(null, parser.getObject(2));

        assertEquals(ColumnType.BOOLEAN, parser.getColumnType(3));
        assertEquals(true, parser.getBoolean(3));
        assertEquals(true, parser.getObject(3));
        assertEquals(false, parser.hasNext());

        List<Row> rows = parser.getRows();
        assertEquals(1, rows.size());
        assertEquals("[[PrimaryKey:]pk0:user0, pk1:100\n[Columns:](Name:col1,Value:true,Timestamp:NotSet)]", rows.toString());
    }

    @Test
    public void testParseMultiRows()
    {
        ByteBuffer buffer = ByteBuffer.allocate(10000);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(SimpleRowMatrixBlockConstants.API_VERSION);
        buffer.putInt(0); // placeholder for data_offset
        buffer.putInt(0); // placeholder for options_offset
        buffer.putInt(2); // pk_count
        buffer.putInt(2); // attr_count
        buffer.putShort((short) 3); // pk0_length
        buffer.put("pk0".getBytes()); // pk0_name
        buffer.putShort((short) 3); // pk1_length
        buffer.put("pk1".getBytes()); // pk1_name
        buffer.putShort((short) 4); // col0_length
        buffer.put("col0".getBytes()); // col0_name
        buffer.putShort((short) 4); // col1_length
        buffer.put("col1".getBytes()); // col1_name
        buffer.putInt(8, buffer.position()); // fill options_offset
        buffer.put(SimpleRowMatrixBlockConstants.TAG_ENTIRE_PRIMARY_KEYS);
        buffer.put((byte) 1); // has entire pk
        buffer.put(SimpleRowMatrixBlockConstants.TAG_ROW_COUNT);
        buffer.putInt(3); // has 0 row
        buffer.putInt(4, buffer.position()); // fill data_offset

        buffer.put(SimpleRowMatrixBlockConstants.TAG_ROW);
        putString(buffer, "user0");
        putLong(buffer, 100L);
        putNull(buffer);
        putBoolean(buffer, true);

        buffer.put(SimpleRowMatrixBlockConstants.TAG_ROW);
        putBlob(buffer, "user1".getBytes());
        putString(buffer, "sk");
        putDouble(buffer, 1.1);
        putNull(buffer);

        buffer.put(SimpleRowMatrixBlockConstants.TAG_ROW);
        putLong(buffer, 200L);
        putString(buffer, "user2");
        putBoolean(buffer, false);
        putLong(buffer, -1L);

        buffer.put(SimpleRowMatrixBlockConstants.TAG_CHECKSUM);
        byte crc = calcCheckSum(buffer);
        buffer.put(crc);

        byte[] bytes = new byte[buffer.position()];
        buffer.position(0);
        buffer.get(bytes);

        SimpleRowMatrixBlockParser parser = new SimpleRowMatrixBlockParser(ByteBuffer.wrap(bytes));
        String[] expectFieldNames = new String[] {"pk0", "pk1", "col0", "col1"};
        String[] fieldNames = parser.parseFieldNames();
        assertEquals(expectFieldNames.length, fieldNames.length);
        for (int i = 0; i < fieldNames.length; i++) {
            assertEquals(expectFieldNames[i], fieldNames[i]);
        }
        assertEquals(2, parser.getPkCount());
        assertEquals(2, parser.getAttrCount());
        assertEquals(4, parser.getFieldCount());
        assertEquals(true, parser.hasEntirePrimaryKeys());
        assertEquals(3, parser.getRowCount());
        assertEquals(true, parser.hasNext());
        assertEquals(buffer.position(), parser.getTotalBytes());
        assertTrue(parser.getDataBytes() > 0);

        List<Row> rows = parser.getRows();
        assertEquals(3, rows.size());
        assertEquals("[[PrimaryKey:]pk0:user0, pk1:100\n[Columns:](Name:col1,Value:true,Timestamp:NotSet), " + "" +
                "[PrimaryKey:]pk0:[117, 115, 101, 114, 49], pk1:sk\n[Columns:](Name:col0,Value:1.1,Timestamp:NotSet), " + "" +
                "[PrimaryKey:]pk0:200, pk1:user2\n[Columns:](Name:col0,Value:false,Timestamp:NotSet)(Name:col1,Value:-1,Timestamp:NotSet)]", rows.toString());
    }
}
