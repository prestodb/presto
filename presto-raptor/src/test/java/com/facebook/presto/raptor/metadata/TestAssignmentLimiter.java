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
package com.facebook.presto.raptor.metadata;

import com.facebook.airlift.testing.TestingTicker;
import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.common.ErrorCodeSupplier;
import com.facebook.presto.common.PrestoException;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.spi.Node;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.HashSet;

import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_NOT_ENOUGH_NODES;
import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_REASSIGNMENT_DELAY;
import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_REASSIGNMENT_THROTTLE;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestAssignmentLimiter
{
    @Test
    public void testLimiter()
    {
        TestingTicker ticker = new TestingTicker();
        AssignmentLimiter limiter = new AssignmentLimiter(
                ImmutableSet::of,
                ticker,
                new Duration(5, MINUTES),
                new Duration(10, MINUTES),
                0);

        // 4:00
        assertCheckFails(limiter, "A", RAPTOR_REASSIGNMENT_DELAY);

        // 4:01
        ticker.increment(1, MINUTES);
        assertCheckFails(limiter, "A", RAPTOR_REASSIGNMENT_DELAY);
        assertCheckFails(limiter, "B", RAPTOR_REASSIGNMENT_DELAY);

        // 4:05
        ticker.increment(4, MINUTES);
        limiter.checkAssignFrom("A");
        assertCheckFails(limiter, "B", RAPTOR_REASSIGNMENT_DELAY);

        // 4:06
        ticker.increment(1, MINUTES);
        assertCheckFails(limiter, "B", RAPTOR_REASSIGNMENT_THROTTLE);
        assertCheckFails(limiter, "C", RAPTOR_REASSIGNMENT_DELAY);

        // 4:14
        ticker.increment(8, MINUTES);
        assertCheckFails(limiter, "B", RAPTOR_REASSIGNMENT_THROTTLE);
        assertCheckFails(limiter, "C", RAPTOR_REASSIGNMENT_THROTTLE);

        // 4:15
        ticker.increment(1, MINUTES);
        limiter.checkAssignFrom("B");
        assertCheckFails(limiter, "C", RAPTOR_REASSIGNMENT_THROTTLE);

        // 4:24
        ticker.increment(9, MINUTES);
        assertCheckFails(limiter, "C", RAPTOR_REASSIGNMENT_THROTTLE);

        // 4:25
        ticker.increment(1, MINUTES);
        limiter.checkAssignFrom("A");

        // 4:30
        ticker.increment(5, MINUTES);
        limiter.checkAssignFrom("A");
        limiter.checkAssignFrom("B");
        limiter.checkAssignFrom("C");
    }

    @Test
    public void testNotEnoughNodes()
            throws Exception
    {
        TestingTicker ticker = new TestingTicker();

        HashSet<Node> nodes = new HashSet<>();
        Node node1 = new InternalNode("node1", new URI("http://127.0.0.1/"), NodeVersion.UNKNOWN, false);
        Node node2 = new InternalNode("node2", new URI("http://127.0.0.2/"), NodeVersion.UNKNOWN, false);
        nodes.add(node1);
        nodes.add(node2);

        AssignmentLimiter limiter = new AssignmentLimiter(
                () -> nodes,
                ticker,
                new Duration(0, SECONDS),
                new Duration(0, SECONDS),
                2);
        ticker.increment(1, SECONDS);

        limiter.checkAssignFrom("node3");

        nodes.remove(node1);
        assertCheckFails(limiter, "node3", RAPTOR_NOT_ENOUGH_NODES);
    }

    private static void assertCheckFails(AssignmentLimiter limiter, String node, ErrorCodeSupplier expected)
    {
        try {
            limiter.checkAssignFrom(node);
            fail("expected exception");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), expected.toErrorCode());
        }
    }
}
