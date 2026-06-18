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
package com.facebook.presto.nativeworker;

import com.facebook.presto.spi.NodeState;
import com.facebook.presto.tests.DistributedQueryRunner;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.NodeState.SHUTTING_DOWN;
import static org.testng.Assert.assertEquals;

public class TestPrestoNativeGracefulShutdown
{
    @Test
    public void testGracefulShutdown() throws Exception
    {
        DistributedQueryRunner distributedQueryRunner = (DistributedQueryRunner) PrestoNativeQueryRunnerUtils.nativeHiveQueryRunnerBuilder()
                .setAddStorageFormatToPath(true)
                .setUseThrift(true)
                .build();

        int responseCode = distributedQueryRunner.sendWorkerRequest(0, "INVALID_BODY");
        assertEquals(responseCode, 400, "Expected a 400 Bad Request response for invalid body");

        responseCode = distributedQueryRunner.sendWorkerRequest(0, "");
        assertEquals(responseCode, 400, "Expected a 400 Bad Request response for empty body");

        responseCode = distributedQueryRunner.sendWorkerRequest(0, "\"SHUTTING_DOWN\"");
        assertEquals(responseCode, 200, "Expected a 200 OK response for valid shutdown request");
        NodeState state = distributedQueryRunner.getWorkerInfoState(0);
        assertEquals(state.getValue(), SHUTTING_DOWN.getValue());

        distributedQueryRunner.close();
    }
}
