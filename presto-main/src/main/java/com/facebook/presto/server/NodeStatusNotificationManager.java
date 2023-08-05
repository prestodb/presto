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
package com.facebook.presto.server;

import com.facebook.presto.spi.nodestatus.NoOpNodeStatusNotificationProvider;
import com.facebook.presto.spi.nodestatus.NodeStatusNotificationProvider;
import com.facebook.presto.spi.nodestatus.NodeStatusNotificationProviderFactory;
import com.google.common.collect.ImmutableMap;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import static com.facebook.presto.util.PropertiesUtil.loadProperties;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class NodeStatusNotificationManager
{
    private static final File NODE_STATUS_NOTIFICATION_CONFIG = new File("etc/node-status-notification.properties");
    private NodeStatusNotificationProviderFactory notificationProviderFactory;
    private NodeStatusNotificationProvider notificationProvider = new NoOpNodeStatusNotificationProvider();
    private boolean isNotificationProviderAdded;

    public void addNodeStatusNotificationProviderFactory(NodeStatusNotificationProviderFactory notificationProviderFactory)
    {
        this.notificationProviderFactory = requireNonNull(notificationProviderFactory, "notificationProviderFactory is null");
    }

    public void loadNodeStatusNotificationProvider()
            throws IOException
    {
        if (this.notificationProviderFactory == null) {
            return;
        }
        checkState(!isNotificationProviderAdded, "NotificationProvider can only be set once");
        this.notificationProvider = this.notificationProviderFactory.create(getConfig());
        this.isNotificationProviderAdded = true;
    }

    private Map<String, String> getConfig()
            throws IOException
    {
        if (NODE_STATUS_NOTIFICATION_CONFIG.exists()) {
            return loadProperties(NODE_STATUS_NOTIFICATION_CONFIG);
        }
        return ImmutableMap.of();
    }

    public NodeStatusNotificationProvider getNotificationProvider()
    {
        return this.notificationProvider;
    }
}
