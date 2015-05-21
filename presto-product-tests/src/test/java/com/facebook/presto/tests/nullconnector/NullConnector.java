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

import org.testng.annotations.Test;

import java.io.IOException;

import static com.facebook.presto.tests.TestGroups.NULL_CONNECTOR;
import static com.teradata.test.assertions.QueryAssert.Row.row;
import static com.teradata.test.assertions.QueryAssert.assertThat;
import static com.teradata.test.query.QueryExecutor.query;
import static java.lang.String.format;

public class NullConnector
{
    @Test(groups = {NULL_CONNECTOR})
    public void nullConnector()
            throws IOException
    {
        String nullTable = "\"null\".default.nation";
        String table = "tpch.tiny.nation";
        try {
            assertThat(query(format("SELECT count(*) from %s", table))).containsExactly(row(25));
            assertThat(query(format("CREATE TABLE %s AS SELECT * FROM %s", nullTable, table)))
                    .updatedRowsCountIsEqualTo(0);
            assertThat(query(format("INSERT INTO %s SELECT * FROM %s", nullTable, table)))
                    .updatedRowsCountIsEqualTo(0);
            assertThat(query(format("SELECT * FROM %s", nullTable))).hasNoRows();
        }
        finally {
            query("DROP TABLE null.default.nation");
        }
    }
}
