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
package com.facebook.presto.spi.nodestatus;

import java.net.InetAddress;

/**
 * The {@code NodeStatusNotificationProvider} interface provides a registry for node status listeners.
 * Implementations of this interface can listen to node status events and notify all registered listeners,
 * especially when a node goes down.
 *
 * <p>It is essential for implementations to ensure proper synchronization if the registry is accessed
 * by multiple threads.</p>
 */
public interface NodeStatusNotificationProvider
{
    void registerGracefulShutdownEventListener(GracefulShutdownEventListener listener);

    void removeGracefulShutdownEventListener(GracefulShutdownEventListener listener);

    void registerRemoteHostShutdownEventListener(InetAddress inetAddress, GracefulShutdownEventListener listener);

    void removeRemoteHostShutdownEventListener(InetAddress inetAddress, GracefulShutdownEventListener listener);
}
