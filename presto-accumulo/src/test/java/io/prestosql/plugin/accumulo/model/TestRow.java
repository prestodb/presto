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
package io.prestosql.plugin.accumulo.model;

import com.google.common.collect.ImmutableList;
import io.prestosql.plugin.accumulo.serializers.AccumuloRowSerializer;
import io.prestosql.spi.type.ArrayType;
import org.testng.annotations.Test;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.GregorianCalendar;
import java.util.Optional;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;

public class TestRow
{
    @Test
    public void testRow()
    {
        Row r1 = new Row();
        r1.addField(new Field(AccumuloRowSerializer.getBlockFromArray(VARCHAR, ImmutableList.of("a", "b", "c")), new ArrayType(VARCHAR)));
        r1.addField(true, BOOLEAN);
        r1.addField(new Field(new Date(new GregorianCalendar(1999, 0, 1).getTime().getTime()), DATE));
        r1.addField(123.45678, DOUBLE);
        r1.addField(new Field(123.45678f, REAL));
        r1.addField(12345678, INTEGER);
        r1.addField(new Field(12345678L, BIGINT));
        r1.addField(new Field((short) 12345, SMALLINT));
        r1.addField(new GregorianCalendar(1999, 0, 1, 12, 30, 0).getTime().getTime(), TIME);
        r1.addField(new Field(new Timestamp(new GregorianCalendar(1999, 0, 1, 12, 30, 0).getTime().getTime()), TIMESTAMP));
        r1.addField((byte) 123, TINYINT);
        r1.addField(new Field("O'Leary".getBytes(UTF_8), VARBINARY));
        r1.addField("O'Leary", VARCHAR);
        r1.addField(null, VARCHAR);

        assertEquals(r1.length(), 14);
        assertEquals(r1.toString(), "(ARRAY ['a','b','c'],true,DATE '1999-01-01',123.45678,123.45678,12345678,12345678,12345,TIME '12:30:00',TIMESTAMP '1999-01-01 12:30:00.0',123,CAST('O''Leary' AS VARBINARY),'O''Leary',null)");

        Row r2 = new Row(r1);
        assertEquals(r2, r1);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "type is null")
    public void testRowTypeIsNull()
    {
        Row r1 = new Row();
        r1.addField(VARCHAR, null);
    }

    @Test
    public void testRowFromString()
    {
        Row expected = new Row();
        expected.addField(new Field(AccumuloRowSerializer.getBlockFromArray(VARCHAR, ImmutableList.of("a", "b", "c")), new ArrayType(VARCHAR)));
        expected.addField(true, BOOLEAN);
        expected.addField(new Field(new Date(new GregorianCalendar(1999, 0, 1).getTime().getTime()), DATE));
        expected.addField(123.45678, DOUBLE);
        expected.addField(new Field(123.45678f, REAL));
        expected.addField(12345678, INTEGER);
        expected.addField(new Field(12345678L, BIGINT));
        expected.addField(new Field((short) 12345, SMALLINT));
        expected.addField(new GregorianCalendar(1999, 0, 1, 12, 30, 0).getTime().getTime(), TIME);
        expected.addField(new Field(new Timestamp(new GregorianCalendar(1999, 0, 1, 12, 30, 0).getTime().getTime()), TIMESTAMP));
        expected.addField((byte) 123, TINYINT);
        expected.addField(new Field("O'Leary".getBytes(UTF_8), VARBINARY));
        expected.addField("O'Leary", VARCHAR);
        expected.addField(null, VARCHAR);

        RowSchema schema = new RowSchema();
        schema.addRowId("a", new ArrayType(VARCHAR));
        schema.addColumn("b", Optional.of("b"), Optional.of("b"), BOOLEAN);
        schema.addColumn("c", Optional.of("c"), Optional.of("c"), DATE);
        schema.addColumn("d", Optional.of("d"), Optional.of("d"), DOUBLE);
        schema.addColumn("e", Optional.of("e"), Optional.of("e"), REAL);
        schema.addColumn("f", Optional.of("f"), Optional.of("f"), INTEGER);
        schema.addColumn("g", Optional.of("g"), Optional.of("g"), BIGINT);
        schema.addColumn("h", Optional.of("h"), Optional.of("h"), SMALLINT);
        schema.addColumn("i", Optional.of("i"), Optional.of("i"), TIME);
        schema.addColumn("j", Optional.of("j"), Optional.of("j"), TIMESTAMP);
        schema.addColumn("k", Optional.of("k"), Optional.of("k"), TINYINT);
        schema.addColumn("l", Optional.of("l"), Optional.of("l"), VARBINARY);
        schema.addColumn("m", Optional.of("m"), Optional.of("m"), VARCHAR);
        schema.addColumn("n", Optional.of("n"), Optional.of("n"), VARCHAR);

        Row actual = Row.fromString(schema, "a,b,c|true|1999-01-01|123.45678|123.45678|12345678|12345678|12345|12:30:00|1999-01-01 12:30:00.0|123|O'Leary|O'Leary|", '|');
        assertEquals(actual, expected);
    }
}
