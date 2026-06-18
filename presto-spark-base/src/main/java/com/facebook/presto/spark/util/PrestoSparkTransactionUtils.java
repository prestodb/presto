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
package com.facebook.presto.spark.util;

import com.facebook.presto.Session;
import com.facebook.presto.transaction.TransactionInfo;
import com.facebook.presto.transaction.TransactionManager;

import java.util.Optional;

import static com.facebook.airlift.concurrent.MoreFutures.getFutureValue;
import static com.google.common.base.Preconditions.checkState;

public class PrestoSparkTransactionUtils
{
    private PrestoSparkTransactionUtils() {}

    public static void commit(Session session, TransactionManager transactionManager)
    {
        getFutureValue(transactionManager.asyncCommit(getTransactionInfo(session, transactionManager).getTransactionId()));
    }

    public static void rollback(Session session, TransactionManager transactionManager)
    {
        getFutureValue(transactionManager.asyncAbort(getTransactionInfo(session, transactionManager).getTransactionId()));
    }

    public static TransactionInfo getTransactionInfo(Session session, TransactionManager transactionManager)
    {
        Optional<TransactionInfo> transaction = session.getTransactionId()
                .flatMap(transactionManager::getOptionalTransactionInfo);
        checkState(transaction.isPresent(), "transaction is not present");
        checkState(transaction.get().isAutoCommitContext(), "transaction doesn't have auto commit context enabled");
        return transaction.get();
    }
}
