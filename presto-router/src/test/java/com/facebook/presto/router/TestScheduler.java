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
import com.facebook.presto.router.scheduler.Scheduler;
import com.facebook.presto.router.scheduler.UserHashScheduler;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.ArrayList;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestScheduler
{
    private final ArrayList<URI> servers = new ArrayList<>();
    private Scheduler scheduler;

    @BeforeClass
    public void setup()
            throws Exception
    {
        servers.add(new URI("192.168.0.1"));
        servers.add(new URI("192.168.0.2"));
        servers.add(new URI("192.168.0.3"));
    }

    @Test
    public void testRandomChoiceScheduler()
            throws Exception
    {
        scheduler = new RandomChoiceScheduler();
        scheduler.setCandidates(servers);

        URI target = scheduler.getDestination("test").orElse(new URI("invalid"));
        assertTrue(servers.contains(target));
    }

    @Test
    public void testUserHashScheduler()
            throws Exception
    {
        scheduler = new UserHashScheduler();
        scheduler.setCandidates(servers);

        URI target1 = scheduler.getDestination("test").orElse(new URI("invalid"));
        assertTrue(servers.contains(target1));

        URI target2 = scheduler.getDestination("test").orElse(new URI("invalid"));
        assertTrue(servers.contains(target2));

        assertEquals(target2, target1);
    }
}
