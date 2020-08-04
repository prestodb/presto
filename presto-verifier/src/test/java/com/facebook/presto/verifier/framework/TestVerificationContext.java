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
package com.facebook.presto.verifier.framework;

import com.facebook.presto.verifier.event.QueryFailure;
import org.testng.annotations.Test;

import java.net.SocketTimeoutException;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.StandardErrorCode.REMOTE_HOST_GONE;
import static com.facebook.presto.verifier.framework.QueryStage.CONTROL_MAIN;
import static com.facebook.presto.verifier.prestoaction.QueryActionStats.EMPTY_STATS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestVerificationContext
{
    private static final String SUITE = "test-suite";
    private static final String NAME = "test-query";
    private static final QueryStage QUERY_STAGE = CONTROL_MAIN;

    @Test
    public void testDuplicateExceptions()
    {
        VerificationContext context = VerificationContext.create(SUITE, NAME);
        QueryException queryException = new PrestoQueryException(new RuntimeException(), false, QUERY_STAGE, Optional.of(REMOTE_HOST_GONE), EMPTY_STATS);

        context.addException(queryException);
        context.addException(queryException);

        List<QueryFailure> queryFailures = context.getQueryFailures();
        assertEquals(queryFailures.size(), 1);
        assertEquals(queryFailures.get(0).getErrorCode(), "PRESTO(REMOTE_HOST_GONE)");
        assertFalse(queryFailures.get(0).isRetryable());
    }

    @Test
    public void testMultipleExceptions()
    {
        VerificationContext context = VerificationContext.create(SUITE, NAME);
        context.addException(new ClusterConnectionException(new SocketTimeoutException(), QUERY_STAGE));
        context.addException(new ClusterConnectionException(new SocketTimeoutException(), QUERY_STAGE));

        List<QueryFailure> queryFailures = context.getQueryFailures();
        assertEquals(queryFailures.size(), 2);
        assertEquals(queryFailures.get(0).getErrorCode(), "CLUSTER_CONNECTION(SocketTimeoutException)");
        assertEquals(queryFailures.get(1).getErrorCode(), "CLUSTER_CONNECTION(SocketTimeoutException)");
        assertTrue(queryFailures.get(0).isRetryable());
        assertTrue(queryFailures.get(1).isRetryable());
    }
}
