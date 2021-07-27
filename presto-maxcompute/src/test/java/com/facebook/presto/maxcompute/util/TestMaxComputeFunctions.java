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
package com.facebook.presto.maxcompute.util;

import com.facebook.presto.Session;
import com.facebook.presto.common.type.SqlTimestamp;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.maxcompute.MaxComputePlugin;
import com.facebook.presto.metadata.FunctionExtractor;
import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class TestMaxComputeFunctions
        extends AbstractTestFunctions
{
    private static final Session SESSION = testSessionBuilder()
            .setCatalog("catalog")
            .setSchema("schema")
            .setSystemProperty("legacy_timestamp", "false")
            .build();

    protected TestMaxComputeFunctions()
    {
        super(SESSION);
    }

    @BeforeClass
    public void setUp()
    {
        functionAssertions.addFunctions(FunctionExtractor.extractFunctions(new MaxComputePlugin().getFunctions()));
    }

    @Test
    public void testToChar()
    {
        assertFunction("to_char(TIME '07:02:09','mi')", VarcharType.VARCHAR, "02");
        assertFunction("to_char(cast(2 as bigint))", VarcharType.VARCHAR, "2");
        assertFunction("to_char(2.3)", VarcharType.VARCHAR, "2.3");
        assertFunction("to_char(true)", VarcharType.VARCHAR, "true");
        assertFunction("to_char(timestamp'2008-07-18 00:00:00', 'yyyymmdd')", VarcharType.VARCHAR, "20080718");
    }

    @Test
    public void testParseUrl()
    {
        assertFunction("parse_url('file://username:password@example.com:8042/over/there/index.dtb?type=animal&name=narwhal#nose', 'HOST')", VarcharType.VARCHAR, "example.com");
        assertFunction("parse_url('file://username:password@example.com:8042/over/there/index.dtb?type=animal&name=narwhal#nose', 'PATH')", VarcharType.VARCHAR, "/over/there/index.dtb");
        assertFunction("parse_url('file://username:password@example.com:8042/over/there/index.dtb?type=animal&name=narwhal#nose', 'QUERY', 'type')", VarcharType.VARCHAR, "animal");
        assertFunction("parse_url('file://username:password@example.com:8042/over/there/index.dtb?type=animal&name=narwhal#nose', 'REF')", VarcharType.VARCHAR, "nose");
        assertFunction("parse_url('file://username:password@example.com:8042/over/there/index.dtb?type=animal&name=narwhal#nose', 'PROTOCOL')", VarcharType.VARCHAR, "file");
        assertFunction("parse_url('file://username:password@example.com:8042/over/there/index.dtb?type=animal&name=narwhal#nose', 'AUTHORITY')", VarcharType.VARCHAR, "username:password@example.com:8042");
        assertFunction("parse_url('file://username:password@example.com:8042/over/there/index.dtb?type=animal&name=narwhal#nose', 'USERINFO')", VarcharType.VARCHAR, "username:password");
    }

    @Test
    public void testDateDiff()
    {
        assertFunction("datediff(timestamp'2006-01-01 00:00:00', timestamp'2005-12-31 23:59:59', 'dd')", BIGINT, 1L);
        assertFunction("datediff(timestamp'2006-01-01 00:00:00', timestamp'2005-12-31 23:59:59', 'mm')", BIGINT, 1L);
        assertFunction("datediff(timestamp'2006-01-01 00:00:00', timestamp'2005-12-31 23:59:59', 'yyyy')", BIGINT, 1L);
        assertFunction("datediff(timestamp'2006-01-01 00:00:00', timestamp'2005-12-31 23:59:59', 'hh')", BIGINT, 1L);
        assertFunction("datediff(timestamp'2006-01-01 00:00:00', timestamp'2005-12-31 23:59:59', 'mi')", BIGINT, 1L);
        assertFunction("datediff(timestamp'2006-01-01 00:00:00', timestamp'2005-12-31 23:59:59', 'ss')", BIGINT, 1L);
    }

    @Test
    public void testDateAdd()
    {
        functionAssertions.assertFunctionString("dateadd(timestamp '2005-02-28 00:00:00', 1, 'dd')", TIMESTAMP,
                    "2005-03-01 00:00:00.000");
        functionAssertions.assertFunctionString("dateadd(timestamp '2005-02-28 00:00:00', -1, 'dd')", TIMESTAMP,
                    "2005-02-27 00:00:00.000");
        functionAssertions.assertFunctionString("dateadd(timestamp '2005-02-28 00:00:00', 20, 'mm')", TIMESTAMP,
                    "2006-10-28 00:00:00.000");
        functionAssertions.assertFunctionString("dateadd(timestamp '2005-02-28 00:00:00', 1, 'mm')", TIMESTAMP,
                    "2005-03-28 00:00:00.000");
        functionAssertions.assertFunctionString("dateadd(timestamp '2005-01-29 00:00:00', 1, 'mm')", TIMESTAMP,
                    "2005-02-28 00:00:00.000");
        functionAssertions.assertFunctionString("dateadd(timestamp '2005-03-30 00:00:00', -1, 'mm')", TIMESTAMP,
                    "2005-02-28 00:00:00.000");
        functionAssertions.assertFunctionString("dateadd(date '2005-02-18', 1, 'mm')", DATE, "2005-03-18");
        functionAssertions.assertFunctionString("dateadd(timestamp '2005-02-18 00:00:00', 1, 'mm')", TIMESTAMP,
                    "2005-03-18 00:00:00.000");
        functionAssertions.assertFunctionString("dateadd(timestamp '2020-11-17 16:31:44',-1,'dd')", TIMESTAMP,
                    "2020-11-16 16:31:44.000");
    }

    @Test
    public void testDateTrunc()
    {
        functionAssertions.assertFunctionString("datetrunc(timestamp'2011-12-07 16:28:46', 'yyyy')", TIMESTAMP,
                "2011-01-01 00:00:00.000");
        functionAssertions.assertFunctionString("datetrunc(timestamp'2011-12-07 16:28:46', 'mm')", TIMESTAMP,
                "2011-12-01 00:00:00.000");
        functionAssertions.assertFunctionString("datetrunc(timestamp'2011-12-07 16:28:46', 'dd')", TIMESTAMP,
                "2011-12-07 00:00:00.000");
        functionAssertions.assertFunctionString("datetrunc(date '2011-12-07', 'yyyy')", DATE,
                "2011-01-01");
        functionAssertions.assertFunctionString("datetrunc(timestamp '2011-12-07 16:28:46', 'yyyy')", TIMESTAMP,
                "2011-01-01 00:00:00.000");
    }

    @Test
    public void testDatePart()
    {
        assertFunction("datepart(timestamp'2013-06-08 01:10:00', 'yyyy')", BIGINT, 2013L);
        assertFunction("datepart(timestamp'2013-06-08 01:10:00', 'mm')", BIGINT, 6L);
        assertFunction("datepart(date '2013-06-08', 'yyyy')", BIGINT, 2013L);
        assertFunction("datepart(timestamp '2013-06-08 01:10:00', 'yyyy')", BIGINT, 2013L);
    }

    @Test
    public void testGetDate()
    {
        assertFunction("getdate()", TIMESTAMP, new SqlTimestamp(session.getStartTime()));
    }
}
