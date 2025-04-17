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
import com.facebook.presto.router.cluster.RemoteStateConfig;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class BarrierClusterManager
        extends ClusterManager
{
    public BarrierClusterManager(RouterConfig config, RemoteInfoFactory remoteInfoFactory, RemoteStateConfig remoteStateConfig, CyclicBarrier barrier)
    {
        super(config, remoteInfoFactory, remoteStateConfig);

        OnConfigChangeDetection parentOnConfigChangeDetection = this.onConfigChangeDetection;
        this.onConfigChangeDetection = () -> {
            try {
                parentOnConfigChangeDetection.apply();
                barrier.await(5, TimeUnit.SECONDS);
            }
            catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
                throw new RuntimeException("Barrier synchronization failed", e);
            }
        };
        startConfigReloadTaskFileWatcher();
    }
}
