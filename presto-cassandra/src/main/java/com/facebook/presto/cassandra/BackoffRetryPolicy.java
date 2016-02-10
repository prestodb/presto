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
package com.facebook.presto.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.WriteType;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.RetryPolicy;

import java.util.concurrent.ThreadLocalRandom;

public class BackoffRetryPolicy
        implements RetryPolicy
{
    public static final BackoffRetryPolicy INSTANCE = new BackoffRetryPolicy();

    private BackoffRetryPolicy() {}

    @Override
    public RetryDecision onUnavailable(Statement statement, ConsistencyLevel consistencyLevel, int requiredReplica, int aliveReplica, int retries)
    {
        if (retries >= 10) {
            return RetryDecision.rethrow();
        }

        try {
            int jitter = ThreadLocalRandom.current().nextInt(100);
            int delay = (100 * (retries + 1)) + jitter;
            Thread.sleep(delay);
            return RetryDecision.retry(consistencyLevel);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return RetryDecision.rethrow();
        }
    }

    @Override
    public RetryDecision onReadTimeout(Statement statement, ConsistencyLevel cl, int requiredResponses, int receivedResponses, boolean dataRetrieved, int nbRetry)
    {
        return DefaultRetryPolicy.INSTANCE.onReadTimeout(statement, cl, requiredResponses, receivedResponses, dataRetrieved, nbRetry);
    }

    @Override
    public RetryDecision onWriteTimeout(Statement statement, ConsistencyLevel cl, WriteType writeType, int requiredAcks, int receivedAcks, int nbRetry)
    {
        return DefaultRetryPolicy.INSTANCE.onWriteTimeout(statement, cl, writeType, requiredAcks, receivedAcks, nbRetry);
    }

    @Override
    public RetryDecision onRequestError(Statement statement, ConsistencyLevel cl, DriverException e, int nbRetry)
    {
        return RetryDecision.tryNextHost(cl);
    }

    @Override
    public void init(Cluster cluster) {}

    @Override
    public void close() {}
}
