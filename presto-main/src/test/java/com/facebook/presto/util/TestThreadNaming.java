package com.facebook.presto.util;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.concurrent.ThreadFactory;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.airlift.concurrent.Threads.threadsNamed;
import static org.testng.Assert.assertEquals;

public class TestThreadNaming
{

    private ThreadFactory threadFactory;
    private ThreadFactory daemonThreadFactory;

    @BeforeClass
    public void setUp()
    {
        threadFactory = threadsNamed("test-%d");
        daemonThreadFactory = daemonThreadsNamed("daemonTest-%d");
    }

    @Test
    public void testThreadNaming()
    {
        Thread firstThread = threadFactory.newThread(() -> System.out.println(Thread.currentThread().getName()));
        assertEquals("test-0", firstThread.getName());

        Thread secondThread = threadFactory.newThread(() -> System.out.println(Thread.currentThread().getName()));
        assertEquals("test-1", secondThread.getName());
    }

    @Test
    public void testDaemonThreadNaming()
    {
        Thread firstThread = daemonThreadFactory.newThread(() -> System.out.println(Thread.currentThread().getName()));
        assertEquals("daemonTest-0", firstThread.getName());

        Thread secondThread = daemonThreadFactory.newThread(() -> System.out.println(Thread.currentThread().getName()));
        assertEquals("daemonTest-1", secondThread.getName());
    }
}
