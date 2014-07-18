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

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.WriteType;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.RetryPolicy;

import io.airlift.log.Logger;

public class PeakNetworkTrafficRetryPolicy implements RetryPolicy
{
    public static final PeakNetworkTrafficRetryPolicy INSTANCE = new PeakNetworkTrafficRetryPolicy();

    private static final Logger log = Logger.get(PeakNetworkTrafficRetryPolicy.class);

    private PeakNetworkTrafficRetryPolicy() {}

    /**
     * Presto often pulls lots of data requiring high network bandwidth,
     * this may result in erroneous exceptions that replicas are not available.
     * It makes sense to retry a few times (10?) in this case.
     */
    @Override
    public RetryDecision onUnavailable(Statement statement,
            ConsistencyLevel cl, int requiredReplica, int aliveReplica,
            int nbRetry)
    {
        if (nbRetry < 10) {
            try {
                Thread.sleep(1000);
            }
            catch (InterruptedException e) {
                log.error("Sleeping 1s has been interrupted, retry was " + nbRetry, e);
            }
            return RetryDecision.retry(cl);
        }
        return RetryDecision.rethrow();
    }

    @Override
    public RetryDecision onReadTimeout(Statement statement,
            ConsistencyLevel cl, int requiredResponses, int receivedResponses,
            boolean dataRetrieved, int nbRetry)
    {
        return DefaultRetryPolicy.INSTANCE.onReadTimeout(statement, cl, requiredResponses, receivedResponses, dataRetrieved, nbRetry);
    }

    @Override
    public RetryDecision onWriteTimeout(Statement statement,
            ConsistencyLevel cl, WriteType writeType, int requiredAcks,
            int receivedAcks, int nbRetry)
    {
        return DefaultRetryPolicy.INSTANCE.onWriteTimeout(statement, cl, writeType, requiredAcks, receivedAcks, nbRetry);
    }
}
