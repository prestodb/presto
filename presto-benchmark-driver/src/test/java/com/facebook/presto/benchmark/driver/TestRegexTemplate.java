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
package com.facebook.presto.benchmark.driver;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static org.testng.Assert.assertEquals;

public class TestRegexTemplate
{
    @Test
    public void test()
            throws Exception
    {
        RegexTemplate regexTemplate = new RegexTemplate("tpch_sf(?<scale>.*?)_(?<format>.*?)_(?<compression>.*?)");

        assertEquals(regexTemplate.getFieldNames(), ImmutableList.of("scale", "format", "compression"));
        assertEquals(regexTemplate.parse("tpch_sf100_orc_zlib"), Optional.of(ImmutableMap.of("scale", "100", "format", "orc", "compression", "zlib")));
        assertEquals(regexTemplate.parse("foo_tpch_sf100_orc_zlib"), Optional.empty());
        assertEquals(regexTemplate.parse("tpch_sf100_orc"), Optional.empty());
        assertEquals(regexTemplate.parse(""), Optional.empty());

        regexTemplate = new RegexTemplate("tpch_sf(?<scale>.*?)_(?<format>.*?)_(?<compression>.*?)\\.sql");
        assertEquals(regexTemplate.parse("tpch_sf100_orc_zlib.sql"), Optional.of(ImmutableMap.of("scale", "100", "format", "orc", "compression", "zlib")));
        assertEquals(regexTemplate.parse("tpch_sf100_orc_zlibXsql"), Optional.empty());
        assertEquals(regexTemplate.parse("tpch_sf100_orc_zlib.sqlFoo"), Optional.empty());
    }
}
