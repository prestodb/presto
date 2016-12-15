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
package com.facebook.presto.teradata.functions;

import com.facebook.presto.Session;
import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.SqlDate;
import com.facebook.presto.spi.type.SqlTimestamp;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.facebook.presto.spi.type.TimestampType;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.metadata.FunctionExtractor.extractFunctions;
import static com.facebook.presto.spi.type.TimeZoneKey.getTimeZoneKey;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.type.TimestampOperators.castToDate;
import static com.facebook.presto.util.DateTimeZoneIndex.getDateTimeZone;
import static java.lang.Math.toIntExact;

public class TestTeradataDateFunctions
        extends AbstractTestFunctions
{
    private static final TimeZoneKey TIME_ZONE_KEY = getTimeZoneKey("Asia/Kathmandu");
    private static final DateTimeZone DATE_TIME_ZONE = getDateTimeZone(TIME_ZONE_KEY);
    private static final Session SESSION = testSessionBuilder()
            .setCatalog("catalog")
            .setSchema("schema")
            .setTimeZoneKey(TIME_ZONE_KEY)
            .build();

    protected TestTeradataDateFunctions()
    {
        super(SESSION);
    }

    @BeforeClass
    public void setUp()
            throws Exception
    {
        functionAssertions.addFunctions(extractFunctions(new TeradataFunctionsPlugin().getFunctions()));
    }

    @Test
    public void testMinimalToDate()
    {
        assertDate("to_date('1988/04/08','yyyy/mm/dd')", 1988, 4, 8);
        assertDate("to_date('04-08-1988','mm-dd-yyyy')", 1988, 4, 8);
        assertDate("to_date('04.1988,08','mm.yyyy,dd')", 1988, 4, 8);
        assertDate("to_date(';198804:08',';yyyymm:dd')", 1988, 4, 8);
    }

    @Test
    public void testMinimalToTimestamp()
    {
        assertTimestamp("to_timestamp('1988/04/08','yyyy/mm/dd')", 1988, 4, 8, 0, 0, 0);
        assertTimestamp("to_timestamp('04-08-1988','mm-dd-yyyy')", 1988, 4, 8, 0, 0, 0);
        assertTimestamp("to_timestamp('04.1988,08','mm.yyyy,dd')", 1988, 4, 8, 0, 0, 0);
        assertTimestamp("to_timestamp(';198804:08',';yyyymm:dd')", 1988, 4, 8, 0, 0, 0);

        assertTimestamp("to_timestamp('1988/04/08 2','yyyy/mm/dd hh')", 1988, 4, 8, 2, 0, 0);

        assertTimestamp("to_timestamp('1988/04/08 14','yyyy/mm/dd hh24')", 1988, 4, 8, 14, 0, 0);
        assertTimestamp("to_timestamp('1988/04/08 14:15','yyyy/mm/dd hh24:mi')", 1988, 4, 8, 14, 15, 0);
        assertTimestamp("to_timestamp('1988/04/08 14:15:16','yyyy/mm/dd hh24:mi:ss')", 1988, 4, 8, 14, 15, 16);

        assertTimestamp("to_timestamp('1988/04/08 2:3:4','yyyy/mm/dd hh24:mi:ss')", 1988, 4, 8, 2, 3, 4);
        assertTimestamp("to_timestamp('1988/04/08 02:03:04','yyyy/mm/dd hh24:mi:ss')", 1988, 4, 8, 2, 3, 4);
    }

    @Test
    public void testMinimalToChar()
    {
        assertVarchar("to_char(TIMESTAMP '1988-04-08 02:03:04','yyyy/mm/dd hh:mi:ss')", "1988/04/08 02:03:04");
        assertVarchar("to_char(TIMESTAMP '1988-04-08 02:03:04','yyyy/mm/dd hh24:mi:ss')", "1988/04/08 02:03:04");
        assertVarchar("to_char(TIMESTAMP '1988-04-08 14:15:16','yyyy/mm/dd hh24:mi:ss')", "1988/04/08 14:15:16");
        assertVarchar("to_char(TIMESTAMP '1988-04-08 14:15:16 +02:09','yyyy/mm/dd hh24:mi:ss')", "1988/04/08 14:15:16");

        assertVarchar("to_char(DATE '1988-04-08','yyyy/mm/dd hh24:mi:ss')", "1988/04/08 00:00:00");
    }

    @Test
    public void testYY()
    {
        assertVarchar("to_char(TIMESTAMP '1988-04-08','yy')", "88");
        assertTimestamp("to_timestamp('88/04/08','yy/mm/dd')", 2088, 4, 8, 0, 0, 0);
        assertDate("to_date('88/04/08','yy/mm/dd')", 2088, 4, 8);
    }

    // TODO: implement this feature SWARM-355
    @Test(enabled = false)
    public void testDefaultValues()
    {
        DateTime current = new DateTime();
        assertDate("to_date('1988','yyyy')", 1988, current.getMonthOfYear(), 1);
        assertDate("to_date('04','mm')", current.getYear(), 4, 1);
        assertDate("to_date('8','dd')", current.getYear(), current.getMonthOfYear(), 8);
    }

    // TODO: implement this feature SWARM-354
    @Test(enabled = false)
    public void testCaseInsensitive()
    {
        assertDate("to_date('1988/04/08','YYYY/MM/DD')", 1988, 4, 8);
        assertDate("to_date('1988/04/08','yYYy/mM/Dd')", 1988, 4, 8);
    }

    @Test
    public void testWhitespace()
    {
        assertDate("to_date('8 04 1988','dd mm yyyy')", 1988, 4, 8);
    }

    // TODO: implement this feature SWARM-353
    @Test(enabled = false)
    public void testEscapedText()
    {
        assertDate("to_date('1988-04-08 TEXT','yyyy-mm-dd \"TEXT\"')", 1988, 4, 8);
    }

    private static SqlDate sqlDate(DateTime from)
    {
        int days = toIntExact(castToDate(SESSION.toConnectorSession(), from.getMillis()));
        return new SqlDate(days);
    }

    private static SqlTimestamp toTimestamp(DateTime dateTime)
    {
        return new SqlTimestamp(dateTime.getMillis(), SESSION.getTimeZoneKey());
    }

    @SuppressWarnings("SameParameterValue")
    private void assertTimestamp(String projection, int year, int month, int day, int hour, int minutes, int seconds)
    {
        assertFunction(
                projection,
                TimestampType.TIMESTAMP,
                toTimestamp(new DateTime(year, month, day, hour, minutes, seconds, DATE_TIME_ZONE)));
    }

    private void assertDate(String projection, int year, int month, int day)
    {
        assertFunction(
                projection,
                DateType.DATE,
                sqlDate(new DateTime(year, month, day, 0, 0, DATE_TIME_ZONE)));
    }

    private void assertVarchar(String projection, String expected)
    {
        assertFunction(projection, VARCHAR, expected);
    }
}
