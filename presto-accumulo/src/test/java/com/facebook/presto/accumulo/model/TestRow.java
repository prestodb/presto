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
package com.facebook.presto.accumulo.model;

import com.facebook.presto.accumulo.serializers.AccumuloRowSerializer;
import com.facebook.presto.type.ArrayType;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.GregorianCalendar;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.FloatType.FLOAT;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;

public class TestRow
{
    @Test
    public void testRow()
            throws Exception
    {
        Row r1 = new Row();
        r1.addField(new Field(AccumuloRowSerializer.getBlockFromArray(VARCHAR, ImmutableList.of("a", "b", "c")), new ArrayType(VARCHAR)));
        r1.addField(true, BOOLEAN);
        r1.addField(new Field(new Date(new GregorianCalendar(1999, 0, 1).getTime().getTime()), DATE));
        r1.addField(123.45678, DOUBLE);
        r1.addField(new Field(123.45678f, FLOAT));
        r1.addField(12345678, INTEGER);
        r1.addField(new Field(12345678L, BIGINT));
        r1.addField(new Field((short) 12345, SMALLINT));
        r1.addField(new GregorianCalendar(1999, 0, 1, 12, 30, 00).getTime().getTime(), TIME);
        r1.addField(new Field(new Timestamp(new GregorianCalendar(1999, 0, 1, 12, 30, 00).getTime().getTime()), TIMESTAMP));
        r1.addField((byte) 123, TINYINT);
        r1.addField(new Field("O'Leary".getBytes(UTF_8), VARBINARY));
        r1.addField("O'Leary", VARCHAR);
        r1.addField(null, VARCHAR);

        assertEquals(r1.length(), 14);
        assertEquals(r1.toString(), "(ARRAY ['a','b','c'],true,DATE '1999-01-01',123.45678,123.45678,12345678,12345678," +
                "12345,TIME '12:30:00',TIMESTAMP '1999-01-01 12:30:00.0',123,CAST('O''Leary' AS VARBINARY),'O''Leary',null)");

        Row r2 = new Row(r1);
        assertEquals(r2, r1);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "type is null")
    public void testRowTypeIsNull()
            throws Exception
    {
        Row r1 = new Row();
        r1.addField(VARCHAR, null);
    }
}
