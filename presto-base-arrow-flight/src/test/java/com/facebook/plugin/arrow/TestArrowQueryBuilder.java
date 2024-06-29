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
package com.facebook.plugin.arrow;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.ColumnHandle;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.testng.Assert.assertEquals;

public class TestArrowQueryBuilder
{
    @Test
    public void testBuildSql()
    {
        String schema = "test_schema";
        String table = "test_table";
        List<ArrowColumnHandle> columns = ImmutableList.of(
                new ArrowColumnHandle("col1", BigintType.BIGINT, new ArrowTypeHandle(1, "type", 0, 0, Optional.empty())),
                new ArrowColumnHandle("col2", VarcharType.VARCHAR, new ArrowTypeHandle(2, "type", 0, 0, Optional.empty())));
        Map<String, String> columnExpressions = new HashMap<>();
        columnExpressions.put("col1", "SUM(col1)");
        columnExpressions.put("col2", "UPPER(col2)");
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.all();
        ArrowQueryBuilder queryBuilder = new ArrowQueryBuilder("'", "`");
        String sql = queryBuilder.buildSql(schema, table, columns, columnExpressions, tupleDomain, Optional.empty());
        assertEquals("SELECT SUM(col1) AS `col1`, UPPER(col2) AS `col2` FROM `test_schema`.`test_table`", sql);
    }
}
