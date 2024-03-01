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
package com.facebook.presto.benchmark.framework;

import org.testng.annotations.Test;

import java.net.SocketTimeoutException;
import java.sql.SQLException;
import java.util.Optional;

import static com.facebook.presto.benchmark.framework.QueryException.Type.CLUSTER_CONNECTION;
import static com.facebook.presto.benchmark.framework.QueryException.Type.PRESTO;
import static com.facebook.presto.spi.StandardErrorCode.REMOTE_TASK_ERROR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestQueryException
{
    @Test
    public void testClusterConnectionTypeException()
    {
        QueryException qe = QueryException.forClusterConnection(new SocketTimeoutException());
        assertEquals(qe.getType(), CLUSTER_CONNECTION);
    }

    @Test
    public void testPrestoTypeException()
    {
        QueryException qe = QueryException.forPresto(new SQLException(), Optional.of(REMOTE_TASK_ERROR), Optional.empty());
        assertEquals(qe.getType(), PRESTO);
        assertTrue(qe.getPrestoErrorCode().isPresent());
    }
}
