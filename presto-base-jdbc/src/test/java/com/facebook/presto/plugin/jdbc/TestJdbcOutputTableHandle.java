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
package com.facebook.presto.plugin.jdbc;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.plugin.jdbc.MetadataUtil.OUTPUT_TABLE_JSON_CODEC;
import static com.facebook.presto.plugin.jdbc.MetadataUtil.OUTPUT_TABLE_THRIFT_CODEC;
import static com.facebook.presto.plugin.jdbc.MetadataUtil.assertJsonRoundTrip;
import static com.facebook.presto.plugin.jdbc.MetadataUtil.assertThriftRoundTrip;

public class TestJdbcOutputTableHandle
{
    private final JdbcOutputTableHandle handle = new JdbcOutputTableHandle(
            "connectorId",
            "catalog",
            "schema",
            "table",
            ImmutableList.of("abc", "xyz"),
            ImmutableList.of(VARCHAR, VARCHAR),
            "tmp_table");
    @Test
    public void testJsonRoundTrip()
    {
        assertJsonRoundTrip(OUTPUT_TABLE_JSON_CODEC, handle);
    }
    @Test
    public void testThriftRoundTrip()
    {
        assertThriftRoundTrip(OUTPUT_TABLE_THRIFT_CODEC, handle);
    }
}
