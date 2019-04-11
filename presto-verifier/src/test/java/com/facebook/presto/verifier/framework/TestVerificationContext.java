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

import com.facebook.presto.verifier.event.FailureInfo;
import org.testng.annotations.Test;

import java.net.SocketTimeoutException;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.StandardErrorCode.REMOTE_HOST_GONE;
import static com.facebook.presto.verifier.framework.QueryOrigin.TargetCluster.CONTROL;
import static com.facebook.presto.verifier.framework.QueryOrigin.forMain;
import static org.testng.Assert.assertEquals;

public class TestVerificationContext
{
    private static final QueryOrigin QUERY_ORIGIN = forMain(CONTROL);

    @Test
    public void testDuplicateExceptions()
    {
        VerificationContext context = new VerificationContext();
        QueryException queryException = QueryException.forPresto(new RuntimeException(), Optional.of(REMOTE_HOST_GONE), false, Optional.empty(), QUERY_ORIGIN);

        context.recordFailure(queryException);
        context.recordFailure(queryException);

        List<FailureInfo> allFailures = context.getAllFailures(CONTROL);
        assertEquals(allFailures.size(), 1);
        assertEquals(allFailures.get(0).getErrorCode(), "PRESTO(REMOTE_HOST_GONE)");
    }

    @Test
    public void testMultipleExceptions()
    {
        VerificationContext context = new VerificationContext();
        context.recordFailure(QueryException.forClusterConnection(new SocketTimeoutException(), QUERY_ORIGIN));
        context.recordFailure(QueryException.forClusterConnection(new SocketTimeoutException(), QUERY_ORIGIN));

        List<FailureInfo> allFailures = context.getAllFailures(CONTROL);
        assertEquals(allFailures.size(), 2);
        assertEquals(allFailures.get(0).getErrorCode(), "CLUSTER_CONNECTION(SocketTimeoutException)");
        assertEquals(allFailures.get(1).getErrorCode(), "CLUSTER_CONNECTION(SocketTimeoutException)");
    }
}
