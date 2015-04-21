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
package com.teradata.presto.functions;

import com.facebook.presto.operator.scalar.FunctionAssertions;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.SqlDate;
import com.facebook.presto.spi.type.Type;
import org.joda.time.DateTime;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static org.joda.time.DateTimeZone.UTC;

public class TestTeradataDateFunctions
{
    private FunctionAssertions functionAssertions;

    @BeforeClass
    public void setUp()
    {
        functionAssertions = new FunctionAssertions().addScalarFunctions(TeradataDateFunctions.class);
    }

    @Test
    public void testMinimalToDate()
    {
        assertDate("to_date('1988/04/08','yyyy/mm/dd')", 1988, 4, 8);
        assertDate("to_date('04-08-1988','mm-dd-yyyy')", 1988, 4, 8);
        assertDate("to_date('04.1988,08','mm.yyyy,dd')", 1988, 4, 8);
        assertDate("to_date(';198804:08',';yyyymm:dd')", 1988, 4, 8);
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

    // TODO: implement this feature SWARM-356
    @Test(enabled = false)
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
        int days = (int) TimeUnit.MILLISECONDS.toDays(from.getMillis());
        return new SqlDate(days);
    }

    private void assertDate(String projection, int year, int month, int day)
    {
        assertFunction(projection, DateType.DATE, sqlDate(new DateTime(year, month, day, 0, 0, UTC)));
    }

    private void assertFunction(String projection, Type expectedType, Object expected)
    {
        functionAssertions.assertFunction(projection, expectedType, expected);
    }
}
