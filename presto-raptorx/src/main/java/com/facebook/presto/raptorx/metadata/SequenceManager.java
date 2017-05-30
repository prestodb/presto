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

import com.facebook.presto.raptorx.util.Database;
import com.facebook.presto.spi.PrestoException;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.statement.GetGeneratedKeys;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import org.jdbi.v3.sqlobject.transaction.Transaction;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.HashMap;
import java.util.Map;

import static com.facebook.presto.raptorx.RaptorErrorCode.RAPTOR_INTERNAL_ERROR;
import static com.facebook.presto.raptorx.util.DatabaseUtil.createJdbi;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

@ThreadSafe
public class SequenceManager
{
    private final SequenceDao dao;

    @GuardedBy("this")
    private final Map<String, CacheEntry> cache = new HashMap<>();

    @Inject
    public SequenceManager(Database database)
    {
        this.dao = createSequenceDao(database);
    }

    public synchronized long nextValue(String name, int cacheCount)
    {
        checkArgument(cacheCount > 0, "cacheCount must be greater than zero");

        CacheEntry entry = cache.get(name);
        if (entry == null) {
            long value = dao.getNext(name, cacheCount);
            if (cacheCount == 1) {
                return value;
            }

            entry = new CacheEntry(value, cacheCount);
            cache.put(name, entry);
        }

        long value = entry.nextValue();
        if (entry.isExhausted()) {
            cache.remove(name);
        }
        return value;
    }

    private static class CacheEntry
    {
        private long nextValue;
        private long remainingValues;

        public CacheEntry(long nextValue, long remainingValues)
        {
            checkArgument(nextValue > 0, "nextValue must be greater than zero");
            checkArgument(remainingValues >= 0, "remainingValues is negative");
            this.nextValue = nextValue;
            this.remainingValues = remainingValues;
        }

        public long nextValue()
        {
            checkState(!isExhausted(), "cache entry is exhausted");
            remainingValues--;
            long value = nextValue;
            nextValue++;
            return value;
        }

        public boolean isExhausted()
        {
            return remainingValues == 0;
        }
    }

    private static SequenceDao createSequenceDao(Database database)
    {
        Jdbi dbi = createJdbi(database.getMasterConnection());
        switch (database.getType()) {
            case H2:
                return dbi.onDemand(H2SequenceDao.class);
            case MYSQL:
                return dbi.onDemand(MySqlSequenceDao.class);
            case POSTGRESQL:
                return dbi.onDemand(PostgreSqlSequenceDao.class);
        }
        throw new PrestoException(RAPTOR_INTERNAL_ERROR, "Unhandled database: " + database.getType());
    }

    public interface SequenceDao
    {
        long getNext(String name, int increment);
    }

    public interface MySqlSequenceDao
            extends SequenceDao
    {
        @Override
        @SqlUpdate("INSERT INTO sequences (sequence_name, next_value)\n" +
                "VALUES (:name, last_insert_id(1) + :increment)\n" +
                "ON DUPLICATE KEY UPDATE\n" +
                "next_value = last_insert_id(next_value) + :increment")
        @GetGeneratedKeys
        long getNext(@Bind String name, @Bind int increment);
    }

    public interface PostgreSqlSequenceDao
            extends SequenceDao
    {
        @Override
        @SqlUpdate("INSERT INTO sequences (sequence_name, next_value)\n" +
                "VALUES (:name, 1 + :increment)\n" +
                "ON CONFLICT (sequence_name) DO UPDATE\n" +
                "SET next_value = sequences.next_value + :increment\n" +
                "RETURNING next_value - :increment")
        @GetGeneratedKeys
        long getNext(@Bind String name, @Bind int increment);
    }

    public interface H2SequenceDao
            extends SequenceDao
    {
        @SqlUpdate("INSERT IGNORE INTO sequences (sequence_name, next_value) VALUES (:name, 1)")
        void insertSequence(@Bind String name);

        @SqlQuery("SELECT next_value FROM sequences WHERE sequence_name = :name FOR UPDATE")
        long getNextValueLocked(@Bind String name);

        @SqlUpdate("UPDATE sequences SET next_value = :value WHERE sequence_name = :name")
        void updateNextValue(@Bind String name, @Bind long value);

        @Override
        @Transaction
        default long getNext(String name, int increment)
        {
            insertSequence(name);
            long value = getNextValueLocked(name);
            updateNextValue(name, value + increment);
            return value;
        }
    }
}
