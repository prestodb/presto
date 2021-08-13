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
package com.facebook.presto.connector.system;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SystemTable;

import java.util.Optional;
import java.util.Set;

public interface SystemTablesProvider
{
    Set<SystemTable> listSystemTables(ConnectorSession session);

    /**
     * Resolves table name. Returns {@link Optional#empty()} if table is not found.
     * Some tables which are not part of set returned by {@link #listSystemTables(ConnectorSession)}
     * can still be validly resolved.
     */
    Optional<SystemTable> getSystemTable(ConnectorSession session, SchemaTableName tableName);
}
