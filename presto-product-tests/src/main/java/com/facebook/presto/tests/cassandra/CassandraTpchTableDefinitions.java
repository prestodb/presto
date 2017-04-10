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
package com.facebook.presto.tests.cassandra;

import com.google.common.collect.ImmutableList;
import com.teradata.tempto.internal.fulfillment.table.cassandra.CassandraTableDefinition;
import com.teradata.tempto.internal.fulfillment.table.cassandra.tpch.CassandraTpchDataSource;
import io.airlift.tpch.TpchTable;

import java.sql.JDBCType;

import static com.facebook.presto.tests.cassandra.TestConstants.CONNECTOR_NAME;
import static com.facebook.presto.tests.cassandra.TestConstants.KEY_SPACE;
import static java.sql.JDBCType.BIGINT;
import static java.sql.JDBCType.DOUBLE;
import static java.sql.JDBCType.VARCHAR;

public class CassandraTpchTableDefinitions
{
    private CassandraTpchTableDefinitions() {}

    public static final ImmutableList<JDBCType> NATION_TYPES = ImmutableList.of(BIGINT, VARCHAR, BIGINT, VARCHAR);

    // TpchTable.NATION does provide data in order: nationkey, name, regionkey, comment. Unfortunately Cassandra reorders columns,
    // so schema will be: nationkey, comment, name, regionkey (primary key first - nationkey, then alphabetical order: comment, name, regionkey)
    // reordering is solved by providing mapping list
    public static final CassandraTableDefinition CASSANDRA_NATION = CassandraTableDefinition.cassandraBuilder("nation")
            .withDatabase(CONNECTOR_NAME)
            .withSchema(KEY_SPACE)
            .setCreateTableDDLTemplate("CREATE TABLE %NAME%(" +
                    "   n_nationkey     BIGINT," +
                    "   n_name          VARCHAR," +
                    "   n_regionkey     BIGINT," +
                    "   n_comment       VARCHAR," +
                    "   primary key(n_nationkey))")
            .setDataSource(new CassandraTpchDataSource(TpchTable.NATION, ImmutableList.of(0, 2, 3, 1), NATION_TYPES, 1.0))
            .build();

    public static final ImmutableList<JDBCType> SUPPLIER_TYPES = ImmutableList.of(BIGINT, VARCHAR, VARCHAR, BIGINT, VARCHAR, DOUBLE, VARCHAR);
    public static final CassandraTableDefinition CASSANDRA_SUPPLIER = CassandraTableDefinition.cassandraBuilder("supplier")
            .withDatabase(CONNECTOR_NAME)
            .withSchema(KEY_SPACE)
            .setCreateTableDDLTemplate("CREATE TABLE %NAME%(" +
                            "   s_suppkey     BIGINT," +
                            "   s_name        VARCHAR," +
                            "   s_address     VARCHAR," +
                            "   s_nationkey   BIGINT," +
                            "   s_phone       VARCHAR," +
                            "   s_acctbal     DOUBLE," +
                            "   s_comment     VARCHAR," +
                            "   primary key(s_suppkey))")
            .setDataSource(new CassandraTpchDataSource(TpchTable.SUPPLIER, ImmutableList.of(0, 4, 2, 5, 6, 1, 3), SUPPLIER_TYPES, 1.0))
            .build();
}
