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
package com.facebook.presto.client;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Base64;
import java.util.List;

import static com.facebook.presto.client.FixJsonDataUtils.fixData;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.google.common.collect.Lists.newArrayList;
import static org.testng.Assert.assertEquals;

public class TestFixJsonDataUtils
{
    @Test
    public void testFixData()
    {
        testFixDataWithTypePrefix("");
    }

    @Test
    public void testFixDataForUserDefinedType()
    {
        testFixDataWithTypePrefix("example.test.type_alt:");
    }

    private void testFixDataWithTypePrefix(String typePrefix)
    {
        assertQueryResult(typePrefix + "bigint", 1000, 1000L);
        assertQueryResult(typePrefix + "integer", 100, 100);
        assertQueryResult(typePrefix + "smallint", 10, (short) 10);
        assertQueryResult(typePrefix + "tinyint", 1, (byte) 1);
        assertQueryResult(typePrefix + "boolean", true, true);
        assertQueryResult(typePrefix + "date", "2017-07-01", "2017-07-01");
        assertQueryResult(typePrefix + "decimal", "2.15", "2.15");
        assertQueryResult(typePrefix + "real", 100.23456, (float) 100.23456);
        assertQueryResult(typePrefix + "double", 100.23456D, 100.23456);
        assertQueryResult(typePrefix + "interval day to second", "INTERVAL '2' DAY", "INTERVAL '2' DAY");
        assertQueryResult(typePrefix + "interval year to month", "INTERVAL '3' MONTH", "INTERVAL '3' MONTH");
        assertQueryResult(typePrefix + "timestamp", "2001-08-22 03:04:05.321", "2001-08-22 03:04:05.321");
        assertQueryResult(typePrefix + "timestamp with time zone", "2001-08-22 03:04:05.321 America/Los_Angeles", "2001-08-22 03:04:05.321 America/Los_Angeles");
        assertQueryResult(typePrefix + "time", "01:02:03.456", "01:02:03.456");
        assertQueryResult(typePrefix + "time with time zone", "01:02:03.456 America/Los_Angeles", "01:02:03.456 America/Los_Angeles");
        assertQueryResult(typePrefix + "varbinary", "garbage", Base64.getDecoder().decode("garbage"));
        assertQueryResult(typePrefix + "varchar", "test string", "test string");
        assertQueryResult(typePrefix + "char", "abc", "abc");
        assertQueryResult(typePrefix + "row(foo bigint,bar bigint)", ImmutableList.of(1, 2), ImmutableMap.of("foo", 1L, "bar", 2L));
        assertQueryResult(typePrefix + "array(bigint)", ImmutableList.of(1, 2, 4), ImmutableList.of(1L, 2L, 4L));
        assertQueryResult(typePrefix + "map(bigint,bigint)", ImmutableMap.of(1, 3, 2, 4), ImmutableMap.of(1L, 3L, 2L, 4L));
        assertQueryResult(typePrefix + "json", "{\"json\": {\"a\": 1}}", "{\"json\": {\"a\": 1}}");
        assertQueryResult(typePrefix + "ipaddress", "1.2.3.4", "1.2.3.4");
        assertQueryResult(typePrefix + "ipprefix", "1.2.3.4/32", "1.2.3.4/32");
        assertQueryResult(typePrefix + "Geometry", "POINT (1.2 3.4)", "POINT (1.2 3.4)");
        assertQueryResult(typePrefix + "map(BingTile,bigint)", ImmutableMap.of("BingTile{x=1, y=2, zoom_level=10}", 1), ImmutableMap.of("BingTile{x=1, y=2, zoom_level=10}", 1L));
        // test nested map structure
        assertQueryResult(typePrefix + "map(map(bigint,bigint),bigint)", ImmutableMap.of("{\n  \"1\" : \"2\",\n  \"3\" : \"4\",\n  \"5\" : \"6\"\n}", "3"), ImmutableMap.of(ImmutableMap.of(1L, 2L, 3L, 4L, 5L, 6L), 3L));
        assertQueryResult(typePrefix + "map(row(foo bigint,bar double),bigint)", ImmutableMap.of("[ \"4\", \"5.0\" ]", "6", "[ \"1\", \"2.0\" ]", "3"), ImmutableMap.of(ImmutableMap.of("foo", 1L, "bar", 2.0), 3L, ImmutableMap.of("foo", 4L, "bar", 5.0), 6L));
        assertQueryResult(typePrefix + "map(array(bigint),bigint)", ImmutableMap.of("[ \"1\", \"3\", \"2\", \"3\" ]", "3"), ImmutableMap.of(ImmutableList.of(1L, 3L, 2L, 3L), 3L));
        // test nested array structure
        assertQueryResult(typePrefix + "array(array(bigint))", ImmutableList.of("[ \"1\", \"2\", \"3\" ]", "[ \"4\", \"5\", \"6\" ]"), ImmutableList.of(ImmutableList.of(1L, 2L, 3L), ImmutableList.of(4L, 5L, 6L)));
        assertQueryResult(typePrefix + "array(map(bigint,bigint))", ImmutableList.of("{\n  \"1\" : \"2\",\n  \"3\" : \"4\"\n}", "{\n  \"5\" : \"6\",\n  \"7\" : \"8\"\n}"), ImmutableList.of(ImmutableMap.of(1L, 2L, 3L, 4L), ImmutableMap.of(5L, 6L, 7L, 8L)));
        assertQueryResult(typePrefix + "array(row(foo bigint,bar double))", ImmutableList.of("[ \"1\", \"2.0\" ]", "[ \"3\", \"4.0\" ]"), ImmutableList.of(ImmutableMap.of("foo", 1L, "bar", 2.0), ImmutableMap.of("foo", 3L, "bar", 4.0)));
        // test nested row structure
        assertQueryResult(typePrefix + "row(foo array(bigint),bar double)", ImmutableList.of("[ \"1\", \"2\" ]", "3.0"), ImmutableMap.of("foo", ImmutableList.of(1L, 2L), "bar", 3.0));
        assertQueryResult(typePrefix + "row(foo map(bigint,bigint),bar double)", ImmutableList.of("{\n  \"1\" : \"2\"\n}", "3.0"), ImmutableMap.of("foo", ImmutableMap.of(1L, 2L), "bar", 3.0));
        assertQueryResult(typePrefix + "row(foo row(x bigint,y double),bar double)", ImmutableList.of("[ \"1\", \"2.0\" ]", "3.0"), ImmutableMap.of("foo", ImmutableMap.of("x", 1L, "y", 2.0), "bar", 3.0));
    }

    private void assertQueryResult(String type, Object data, Object expected)
    {
        List<List<Object>> rows = newArrayList(fixData(
                ImmutableList.of(new Column("test", parseTypeSignature(type))),
                ImmutableList.of(ImmutableList.of(data))));
        assertEquals(rows.size(), 1);
        assertEquals(rows.get(0).size(), 1);
        assertEquals(rows.get(0).get(0), expected);
    }
}
