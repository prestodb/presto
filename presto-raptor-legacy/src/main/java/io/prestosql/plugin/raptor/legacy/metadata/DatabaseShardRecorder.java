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
package io.prestosql.plugin.raptor.legacy.metadata;

import io.airlift.log.Logger;
import io.prestosql.plugin.raptor.legacy.util.DaoSupplier;
import io.prestosql.spi.PrestoException;

import javax.inject.Inject;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static io.prestosql.plugin.raptor.legacy.util.DatabaseUtil.metadataError;
import static io.prestosql.plugin.raptor.legacy.util.DatabaseUtil.runIgnoringConstraintViolation;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class DatabaseShardRecorder
        implements ShardRecorder
{
    private static final Logger log = Logger.get(DatabaseShardRecorder.class);

    private final ShardDao dao;

    @Inject
    public DatabaseShardRecorder(DaoSupplier<ShardDao> shardDaoSupplier)
    {
        this.dao = shardDaoSupplier.onDemand();
    }

    @Override
    public void recordCreatedShard(long transactionId, UUID shardUuid)
    {
        int maxAttempts = 5;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                runIgnoringConstraintViolation(() -> dao.insertCreatedShard(shardUuid, transactionId));
                return;
            }
            catch (PrestoException e) {
                if (attempt == maxAttempts) {
                    throw e;
                }
                log.warn(e, "Failed to insert created shard on attempt %s, will retry", attempt);
                try {
                    long millis = attempt * 2000L;
                    MILLISECONDS.sleep(millis + ThreadLocalRandom.current().nextLong(0, millis));
                }
                catch (InterruptedException ie) {
                    throw metadataError(ie);
                }
            }
        }
    }
}
