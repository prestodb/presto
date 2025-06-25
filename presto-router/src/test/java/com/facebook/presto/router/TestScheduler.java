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
import com.facebook.presto.router.scheduler.UserHashScheduler;
import com.facebook.presto.router.scheduler.WeightedRandomChoiceScheduler;
import com.facebook.presto.router.scheduler.WeightedRoundRobinScheduler;
import com.facebook.presto.spi.router.RouterRequestInfo;
import com.facebook.presto.spi.router.Scheduler;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestScheduler
{
    private final ArrayList<URI> servers = new ArrayList<>();

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
    }

    @Test
    public void testRandomChoiceScheduler()
            throws Exception
    {
        Scheduler scheduler = new RandomChoiceScheduler();
        scheduler.setCandidates(servers);

        HashMap<URI, Integer> hitCounter = new HashMap<>();

        for (int i = 0; i < 10_000; i++) {
            URI target = scheduler.getDestination(new RouterRequestInfo("test")).orElse(new URI("invalid"));
            assertTrue(servers.contains(target));
            hitCounter.put(target, hitCounter.getOrDefault(target, 0) + 1);
        }

        assertEquals(((float) hitCounter.get(servers.get(1)) / hitCounter.get(servers.get(0))), 1, 1E-1);
        assertEquals(((float) hitCounter.get(servers.get(2)) / hitCounter.get(servers.get(1))), 1, 1E-1);
    }

    @DataProvider(name = "users")
    public Object[][] provideUsers()
    {
        return new Object[][]
                {
                        {"test"}, {"user"}, {"1234"},
                };
    }

    @Test(dataProvider = "users")
    public void testUserHashScheduler(String user)
            throws Exception
    {
        Scheduler scheduler = new UserHashScheduler();
        scheduler.setCandidates(servers);

        URI target1 = scheduler.getDestination(new RouterRequestInfo(user)).orElse(new URI("invalid"));
        assertTrue(servers.contains(target1));

        URI target2 = scheduler.getDestination(new RouterRequestInfo(user)).orElse(new URI("invalid"));
        assertTrue(servers.contains(target2));

        URI target3 = scheduler.getDestination(new RouterRequestInfo(user)).orElse(new URI("invalid"));
        assertTrue(servers.contains(target3));

        assertEquals(target2, target1);
        assertEquals(target3, target2);
    }

    @Test
    public void testWeightedRandomChoiceScheduler()
            throws Exception
    {
        HashMap<URI, Integer> weights = new HashMap<>();
        Scheduler scheduler = new WeightedRandomChoiceScheduler();
        scheduler.setCandidates(servers);

        weights.put(servers.get(0), 1);
        weights.put(servers.get(1), 3);
        weights.put(servers.get(2), 9);
        scheduler.setWeights(weights);

        HashMap<URI, Integer> hitCounter = new HashMap<>();

        for (int i = 0; i < 100_000; i++) {
            URI target = scheduler.getDestination(new RouterRequestInfo("test")).orElse(new URI("invalid"));
            assertTrue(servers.contains(target));
            assertTrue(weights.containsKey(target));
            hitCounter.put(target, hitCounter.getOrDefault(target, 0) + 1);
        }

        //testing that ratios between weights roughly equate to ratios between actual destinations
        assertEquals(((float) hitCounter.get(servers.get(1)) / hitCounter.get(servers.get(0))), 3, 5E-1);
        assertEquals(((float) hitCounter.get(servers.get(2)) / hitCounter.get(servers.get(1))), 3, 5E-1);
        assertEquals(((float) hitCounter.get(servers.get(2)) / hitCounter.get(servers.get(0))), 9, 5E-1);
    }

    @Test
    public void testWeightedRandomChoiceSchedulerZeroWeight()
            throws Exception
    {
        HashMap<URI, Integer> weights = new HashMap<>();
        Scheduler scheduler = new WeightedRandomChoiceScheduler();
        weights.put(servers.get(0), 0);
        weights.put(servers.get(1), 0);
        weights.put(servers.get(2), 0);
        scheduler.setWeights(weights);
        scheduler.setWeights(weights);

        scheduler.setCandidates(servers);

        URI target = scheduler.getDestination(new RouterRequestInfo("test")).orElse(new URI("invalid"));
        assertEquals(target.getPath(), "192.168.0.1");
    }

    @Test
    public void testRoundRobinScheduler()
            throws Exception
    {
        Scheduler scheduler = new RoundRobinScheduler();
        scheduler.setCandidates(servers);
        scheduler.setCandidateGroupName("");

        URI target1 = scheduler.getDestination(new RouterRequestInfo("test")).orElse(new URI("invalid"));
        assertTrue(servers.contains(target1));
        assertEquals(target1.getPath(), "192.168.0.1");

        URI target2 = scheduler.getDestination(new RouterRequestInfo("test")).orElse(new URI("invalid"));
        assertTrue(servers.contains(target2));
        assertEquals(target2.getPath(), "192.168.0.2");

        URI target3 = scheduler.getDestination(new RouterRequestInfo("test")).orElse(new URI("invalid"));
        assertTrue(servers.contains(target3));
        assertEquals(target3.getPath(), "192.168.0.3");

        URI target4 = scheduler.getDestination(new RouterRequestInfo("test")).orElse(new URI("invalid"));
        assertTrue(servers.contains(target4));
        assertEquals(target4.getPath(), "192.168.0.1");
    }

    @DataProvider(name = "weights")
    public Object[][] provideWeights()
    {
        return new Object[][]
                {
                        {1, 10, 100},
                        {1, 5, 10},
                        {10, 20, 30},
                };
    }

    @Test(dataProvider = "weights")
    public void testWeightedRoundRobinScheduler(int... weightArr)
            throws Exception
    {
        HashMap<URI, Integer> weights = new HashMap<>();
        Scheduler scheduler = new WeightedRoundRobinScheduler();
        scheduler.setCandidates(servers);

        weights.put(servers.get(0), weightArr[0]);
        weights.put(servers.get(1), weightArr[1]);
        weights.put(servers.get(2), weightArr[2]);

        scheduler.setWeights(weights);
        scheduler.setCandidateGroupName("");

        //explicitly checking that each server is accessed repeatedly a number of times equal to their assigned weight
        int weightSum = Arrays.stream(weightArr).sum();
        testScheduler(weightSum, scheduler, weights);
    }

    private void testScheduler(int weightSum, Scheduler scheduler, HashMap<URI, Integer> weights)
            throws URISyntaxException
    {
        int serverDiffCount = 0;
        int serverRepeatCount = 0;
        URI priorURI = null;
        for (int i = 0; i < weightSum; i++) {
            URI target = scheduler.getDestination(new RouterRequestInfo("test")).orElse(new URI("invalid"));
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
