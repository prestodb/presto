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

        URI target = scheduler.getDestination("test").orElse(new URI("invalid"));
        assertTrue(servers.contains(target));
    }

    @Test
    public void testUserHashScheduler()
            throws Exception
    {
        Scheduler scheduler = new UserHashScheduler();
        scheduler.setCandidates(servers);

        URI target1 = scheduler.getDestination("test").orElse(new URI("invalid"));
        assertTrue(servers.contains(target1));

        URI target2 = scheduler.getDestination("test").orElse(new URI("invalid"));
        assertTrue(servers.contains(target2));

        assertEquals(target2, target1);
    }

    @Test
    public void testWeightedRandomChoiceScheduler()
            throws Exception
    {
        Scheduler scheduler = new WeightedRandomChoiceScheduler();
        scheduler.setCandidates(servers);
        scheduler.setWeights(weights);

        HashMap<URI, Integer> hitCounter = new HashMap<>();

        for (int i = 0; i < 1000; i++) {
            URI target = scheduler.getDestination("test").orElse(new URI("invalid"));
            assertTrue(servers.contains(target));
            assertTrue(weights.containsKey(target));
            hitCounter.put(target, hitCounter.getOrDefault(target, 0) + 1);
        }

        assertTrue(hitCounter.get(new URI("192.168.0.3")) > hitCounter.get(new URI("192.168.0.1")));
    }

    @Test
    public void testRoundRobinScheduler()
            throws Exception
    {
        Scheduler scheduler = new RoundRobinScheduler();
        scheduler.setCandidates(servers);

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
}
