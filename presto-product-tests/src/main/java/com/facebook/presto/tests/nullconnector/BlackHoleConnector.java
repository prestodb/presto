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

package com.facebook.presto.tests.nullconnector;

import com.teradata.tempto.query.QueryResult;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.UUID;

import static com.facebook.presto.tests.TestGroups.BLACKHOLE_CONNECTOR;
import static com.teradata.tempto.assertions.QueryAssert.Row.row;
import static com.teradata.tempto.assertions.QueryAssert.assertThat;
import static com.teradata.tempto.query.QueryExecutor.query;
import static java.lang.String.format;

public class BlackHoleConnector
{
    @Test(groups = {BLACKHOLE_CONNECTOR})
    public void blackHoleConnector()
            throws IOException
    {
        String nullTable = "\"blackhole\".default.nation_" + UUID.randomUUID().toString().replace("-", "");
        String table = "tpch.tiny.nation";

        assertThat(query(format("SELECT count(*) from %s", table))).containsExactly(row(25));
        QueryResult result = query(format("CREATE TABLE %s AS SELECT * FROM %s", nullTable, table));
        try {
            assertThat(result).updatedRowsCountIsEqualTo(25);
            assertThat(query(format("INSERT INTO %s SELECT * FROM %s", nullTable, table)))
                    .updatedRowsCountIsEqualTo(25);
            assertThat(query(format("SELECT * FROM %s", nullTable))).hasNoRows();
        }
        finally {
            query(format("DROP TABLE %s", nullTable));
        }
    }
}
