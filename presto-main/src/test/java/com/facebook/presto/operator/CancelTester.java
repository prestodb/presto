/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.tuple.TupleInfo;
import org.testng.Assert;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public final class CancelTester
{
    public CancelTester()
    {
    }

    private static final ExecutorService executor = Executors.newCachedThreadPool();

    public static BlockingOperator createCancelableDataSource(TupleInfo... tupleInfos)
    {
        Operator dataSource = new ExceptionOperator(tupleInfos);
        return new BlockingOperator(dataSource);
    }

    public static BlockingOperator createCancelableDataSource(Page page, int maxPages, TupleInfo... tupleInfos)
    {
        Operator dataSource = new ExceptionOperator(page, maxPages, tupleInfos);
        return new BlockingOperator(dataSource);
    }

    public static void assertCancel(Operator testOperator, BlockingOperator blockingOperator)
            throws Exception
    {
        // create the iterator while the gate is unlocked
        // to cause operators that call next in the constructor to fail
        blockingOperator.getGate().unlock();
        final PageIterator iterator = testOperator.iterator(new OperatorStats());
        blockingOperator.getGate().lock();

        Future<?> future = executor.submit(new Callable<Boolean>()
        {
            @Override
            public Boolean call()
            {
                Assert.assertFalse(iterator.hasNext());
                Assert.assertFalse(iterator.hasNext());
                return true;
            }
        });

        // block until the thread above is waiting in the blocking iterator
        blockingOperator.getGate().awaitWaiters(1);

        // close the iterator
        iterator.close();

        // unlock the gate
        blockingOperator.getGate().unlock();

        // the task above should have completed successfully
        Assert.assertTrue((Boolean) future.get(1, TimeUnit.SECONDS));
    }
}
