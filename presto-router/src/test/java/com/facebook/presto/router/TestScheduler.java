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
package com.facebook.presto.router;

import com.facebook.presto.router.scheduler.RandomChoiceScheduler;
import com.facebook.presto.router.scheduler.RoundRobinScheduler;
import com.facebook.presto.router.scheduler.Scheduler;
import com.facebook.presto.router.scheduler.UserHashScheduler;
import com.facebook.presto.router.scheduler.WeightedRandomChoiceScheduler;
import com.facebook.presto.router.scheduler.WeightedRoundRobinScheduler;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestScheduler
{
    private final ArrayList<URI> servers = new ArrayList<>();
    private final HashMap<URI, Integer> weights = new HashMap<>();

    @BeforeClass
    public void setup()
            throws Exception
    {
        URI uri1 = new URI("192.168.0.1");
        URI uri2 = new URI("192.168.0.2");
        URI uri3 = new URI("192.168.0.3");

        servers.add(uri1);
        servers.add(uri2);
        servers.add(uri3);

        weights.put(uri1, 1);
        weights.put(uri2, 10);
        weights.put(uri3, 100);
    }

    @Test
    public void testRandomChoiceScheduler()
            throws Exception
    {
        Scheduler scheduler = new RandomChoiceScheduler();
        scheduler.setCandidates(servers);

        HashMap<URI, Integer> hitCounter = new HashMap<>();

        for (int i = 0; i < 100_000; i++) {
            URI target = scheduler.getDestination("test").orElse(new URI("invalid"));
            assertTrue(servers.contains(target));
            hitCounter.put(target, hitCounter.getOrDefault(target, 0) + 1);
        }

        //testing that each destination is hit a similar number of times over 100,000 queries
        assertTrue(((float) hitCounter.get(servers.get(1)) / hitCounter.get(servers.get(0)) > 0.8));
        assertTrue(((float) hitCounter.get(servers.get(1)) / hitCounter.get(servers.get(0)) < 1.2));
        assertTrue(((float) hitCounter.get(servers.get(2)) / hitCounter.get(servers.get(1)) > 0.8));
        assertTrue(((float) hitCounter.get(servers.get(2)) / hitCounter.get(servers.get(1)) < 1.2));
    }

    @Test
    public void testUserHashScheduler()
            throws Exception
    {
        Scheduler scheduler = new UserHashScheduler();
        scheduler.setCandidates(servers);

        String[] users = {"test", "user", "1234"};
        for (String user : users) {
            URI target1 = scheduler.getDestination(user).orElse(new URI("invalid"));
            assertTrue(servers.contains(target1));

            URI target2 = scheduler.getDestination(user).orElse(new URI("invalid"));
            assertTrue(servers.contains(target2));

            URI target3 = scheduler.getDestination(user).orElse(new URI("invalid"));
            assertTrue(servers.contains(target3));

            assertEquals(target2, target1);
            assertEquals(target3, target2);
        }
    }

    @Test
    public void testWeightedRandomChoiceScheduler()
            throws Exception
    {
        Scheduler scheduler = new WeightedRandomChoiceScheduler();
        scheduler.setCandidates(servers);
        scheduler.setWeights(weights);

        HashMap<URI, Integer> hitCounter = new HashMap<>();

        for (int i = 0; i < 100_000; i++) {
            URI target = scheduler.getDestination("test").orElse(new URI("invalid"));
            assertTrue(servers.contains(target));
            assertTrue(weights.containsKey(target));
            hitCounter.put(target, hitCounter.getOrDefault(target, 0) + 1);
        }

        //testing that ratios between weights roughly equate to ratios between actual destinations, may not pass 100% of the time
        assertTrue((hitCounter.get(servers.get(1)) / hitCounter.get(servers.get(0)) > 8));
        assertTrue((hitCounter.get(servers.get(1)) / hitCounter.get(servers.get(0)) < 12));
        assertTrue((hitCounter.get(servers.get(2)) / hitCounter.get(servers.get(1)) > 8));
        assertTrue((hitCounter.get(servers.get(2)) / hitCounter.get(servers.get(1)) < 12));
    }

    @Test
    public void testRoundRobinScheduler()
            throws Exception
    {
        Scheduler scheduler = new RoundRobinScheduler();
        scheduler.setCandidates(servers);
        scheduler.setCandidateGroupName("");

        URI target1 = scheduler.getDestination("test").orElse(new URI("invalid"));
        assertTrue(servers.contains(target1));
        assertEquals(target1.getPath(), "192.168.0.1");

        URI target2 = scheduler.getDestination("test").orElse(new URI("invalid"));
        assertTrue(servers.contains(target2));
        assertEquals(target2.getPath(), "192.168.0.2");

        URI target3 = scheduler.getDestination("test").orElse(new URI("invalid"));
        assertTrue(servers.contains(target3));
        assertEquals(target3.getPath(), "192.168.0.3");

        URI target4 = scheduler.getDestination("test").orElse(new URI("invalid"));
        assertTrue(servers.contains(target4));
        assertEquals(target4.getPath(), "192.168.0.1");
    }

    @Test
    public void testWeightedRoundRobinScheduler()
            throws Exception
    {
        Scheduler scheduler = new WeightedRoundRobinScheduler();
        scheduler.setCandidates(servers);
        scheduler.setWeights(weights);
        scheduler.setCandidateGroupName("");

        //explicitly checking that each server is accessed repeatedly a number of times equal to their assigned weight
        int serverDiffCount = 0;
        int serverRepeatCount = 0;
        URI priorURI = null;
        for (int i = 0; i < 111; i++) {
            URI target = scheduler.getDestination("test").orElse(new URI("invalid"));
            assertTrue(servers.contains(target));
            assertTrue(weights.containsKey(target));
            if (!target.equals(priorURI)) {
                assertEquals(serverRepeatCount, weights.getOrDefault(priorURI, 0));
                serverRepeatCount = 1;
                serverDiffCount++;
                priorURI = target;
            }
            else {
                serverRepeatCount++;
            }
        }
        assertEquals(serverDiffCount, servers.size());
    }
}
