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
package com.facebook.presto.spi;

public interface SystemTable
{
    /**
     * True if table is distributed across all nodes.
     */
    boolean isDistributed();

    ConnectorTableMetadata getTableMetadata();

    /**
     * Create a cursor for the data in this table.
     *
     * @param session the session to use for creating the data
     * @param constraint the constraints for the table columns (indexed from 0)
     */
    RecordCursor cursor(ConnectorSession session, TupleDomain<Integer> constraint);
}
