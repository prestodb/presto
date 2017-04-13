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
package com.facebook.presto.tests.sqlserver;

import com.teradata.tempto.fulfillment.table.jdbc.RelationalTableDefinition;
import com.teradata.tempto.fulfillment.table.jdbc.tpch.JdbcTpchTableDefinitions;

import static com.facebook.presto.tests.sqlserver.TestConstants.CONNECTOR_NAME;

public class SqlServerTpchTableDefinitions
{
    private SqlServerTpchTableDefinitions() {}

    public static final RelationalTableDefinition NATION = RelationalTableDefinition.like(JdbcTpchTableDefinitions.NATION).withDatabase(CONNECTOR_NAME).build();
}
