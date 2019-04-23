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
package com.facebook.presto.raptorx;

import com.facebook.presto.raptorx.metadata.Metadata;
import com.facebook.presto.raptorx.transaction.Transaction;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import io.airlift.log.Logger;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.HashMap;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;

import static com.facebook.presto.raptorx.RaptorErrorCode.RAPTOR_INTERNAL_ERROR;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class TransactionManager
{
    private static final Logger log = Logger.get(TransactionManager.class);

    private final Metadata metadata;

    @GuardedBy("this")
    private final Map<ConnectorTransactionHandle, Transaction> transactions = new HashMap<>();

    @Inject
    public TransactionManager(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    public synchronized ConnectorTransactionHandle create()
    {
        long transactionId = metadata.nextTransactionId();
        long commitId = metadata.getCurrentCommitId();
        Transaction transaction = new Transaction(metadata, transactionId, commitId);

        RaptorTransactionHandle handle = new RaptorTransactionHandle(transactionId, commitId);
        transactions.put(handle, transaction);
        log.debug("ActiveCommitId add: " + transaction.getTransactionId() + ", " + transactions.size() + ", " + transaction.getCommitId());
        return handle;
    }

    public synchronized Transaction get(ConnectorTransactionHandle handle)
    {
        Transaction transaction = transactions.get(handle);
        if (transaction == null) {
            throw new PrestoException(RAPTOR_INTERNAL_ERROR, "No such transaction: " + handle);
        }
        return transaction;
    }

    public synchronized void remove(ConnectorTransactionHandle handle)
    {
        Transaction transaction = transactions.get(handle);
        transactions.remove(handle);
        log.debug("ActiveCommitId remove: " + transaction.getTransactionId() + ", " + transactions.size() + ", " + transaction.getCommitId());
    }

    public synchronized long oldestActiveCommitId()
    {
        OptionalLong m = transactions.values().stream()
                .mapToLong(Transaction::getCommitId)
                .min();
        if (m.isPresent()) {
            log.debug("oldestActiveCommitId in mem: " + m.getAsLong());
        }
        else {
            log.debug("oldestActiveCommitId go to db");
        }
        return transactions.values().stream()
                .mapToLong(Transaction::getCommitId)
                .min()
                .orElseGet(metadata::getCurrentCommitId);
    }

    public synchronized Set<Long> activeTransactions()
    {
        return transactions.values().stream()
                .map(Transaction::getTransactionId)
                .collect(toImmutableSet());
    }
}
