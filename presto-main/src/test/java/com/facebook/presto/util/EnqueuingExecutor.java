package com.facebook.presto.util;

import com.google.common.base.Preconditions;

import javax.annotation.concurrent.ThreadSafe;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;

@ThreadSafe
public class EnqueuingExecutor
    implements Executor
{
    private final BlockingQueue<Runnable> taskQueue = new LinkedBlockingQueue<>();

    @Override
    public void execute(Runnable task)
    {
        Preconditions.checkNotNull(task, "task is null");
        taskQueue.add(task);
    }

    public Runnable removeNext()
    {
        return taskQueue.remove();
    }

    public void runNext()
    {
        removeNext().run();
    }

    public void runNextN(int n)
    {
        for (int i = 0; i < n; i++) {
            runNext();
        }
    }

    public void runAll()
    {
        ArrayList<Runnable> tasks = new ArrayList<>();
        taskQueue.drainTo(tasks);
        for (Runnable task : tasks) {
            task.run();
        }
    }

    public int size()
    {
        return taskQueue.size();
    }

    public boolean isEmpty()
    {
        return taskQueue.isEmpty();
    }
}
