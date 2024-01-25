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
package com.facebook.presto.operator;

import com.facebook.presto.spi.nodestatus.GracefulShutdownEventListener;
import com.facebook.presto.spi.nodestatus.NodeStatusNotificationProvider;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class TestNodeStatusNotifier
        implements NodeStatusNotificationProvider
{
    // The ExchangeClient would pass the lambda closure so has to use strong reference.
    HashMap<InetAddress, GracefulShutdownEventListener> remoteHostShutdownEventListeners;
    Set<GracefulShutdownEventListener> gracefulShutdownEventListeners;

    public TestNodeStatusNotifier()
    {
        this.gracefulShutdownEventListeners = new HashSet<>();
        this.remoteHostShutdownEventListeners = new HashMap<>();
    }

    @Override
    public void registerGracefulShutdownEventListener(GracefulShutdownEventListener listener)
    {
        this.gracefulShutdownEventListeners.add(listener);
    }

    @Override
    public void removeGracefulShutdownEventListener(GracefulShutdownEventListener listener)
    {
        this.gracefulShutdownEventListeners.remove(listener);
    }

    @Override
    public void registerRemoteHostShutdownEventListener(InetAddress inetAddress, GracefulShutdownEventListener listener)
    {
        this.remoteHostShutdownEventListeners.put(inetAddress, listener);
    }

    @Override
    public void removeRemoteHostShutdownEventListener(InetAddress inetAddress, GracefulShutdownEventListener listener)
    {
        this.remoteHostShutdownEventListeners.remove(inetAddress);
    }

    public void workerShutdown(InetAddress workerHostAddress)
    {
        GracefulShutdownEventListener listener = this.remoteHostShutdownEventListeners.get(workerHostAddress);
        if (listener != null) {
            listener.onNodeShuttingDown();
        }
    }
}
