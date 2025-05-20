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

import com.facebook.presto.router.cluster.ClusterManager;
import com.facebook.presto.router.cluster.RemoteInfoFactory;
import com.facebook.presto.router.scheduler.CustomSchedulerManager;

import java.io.IOException;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class BarrierClusterManager
        extends ClusterManager
{
    private final CyclicBarrier barrier;

    public BarrierClusterManager(RouterConfig config, RemoteInfoFactory remoteInfoFactory, CyclicBarrier barrier, CustomSchedulerManager schedulerManager)
            throws IOException
    {
        super(config, remoteInfoFactory, schedulerManager);
        this.barrier = barrier;
    }

    @Override
    protected void reloadConfig()
    {
        try {
            super.reloadConfig();
            if (barrier != null) {
                barrier.await(5, TimeUnit.SECONDS);
            }
        }
        catch (BrokenBarrierException | InterruptedException | TimeoutException e) {
            throw new RuntimeException("Barrier synchronization failed", e);
        }
    }
}
