package com.facebook.presto.router;

import com.facebook.presto.router.scheduler.RandomChoiceScheduler;
import com.facebook.presto.router.scheduler.Scheduler;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.ArrayList;

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
}
