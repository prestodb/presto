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
package com.facebook.presto.raptorx.metadata;

import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

public interface MasterTransactionDao
{
    @SqlUpdate("INSERT INTO transactions (transaction_id, start_time)\n" +
            "VALUES (:transactionId, :startTime)")
    void insertTransaction(
            @Bind long transactionId,
            @Bind long startTime);

    @SqlUpdate("INSERT INTO transaction_tables (transaction_id, table_id)\n" +
            "VALUES (:transactionId, :tableId)")
    void insertTransactionTable(
            @Bind long transactionId,
            @Bind long tableId);
}
