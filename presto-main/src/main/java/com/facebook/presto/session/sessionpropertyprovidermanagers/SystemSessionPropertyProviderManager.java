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
package com.facebook.presto.session.sessionpropertyprovidermanagers;

import com.facebook.presto.spi.session.SystemSessionPropertyProvider;
import com.facebook.presto.spi.session.SystemSessionPropertyProviderFactory;
import com.google.common.collect.ImmutableMap;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import static com.facebook.presto.util.PropertiesUtil.loadProperties;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class SystemSessionPropertyProviderManager
{
    private static final File SESSION_PROPERTY_PROVIDER_CONFIG = new File("etc/session-property-provider.properties");
    private SystemSessionPropertyProviderFactory providerFactory;
    private SystemSessionPropertyProvider provider;
    private boolean isSessionProviderAdded;

    public void addSessionPropertyProviderFactory(SystemSessionPropertyProviderFactory providerFactory)
    {
        this.providerFactory = requireNonNull(providerFactory, "providerFactory is null");
    }

    public void loadSessionPropertyProvider()
            throws IOException
    {
        if (this.providerFactory == null) {
            return;
        }
        checkState(!isSessionProviderAdded, "SystemSessionPropertyProvider can only be set once");
        this.provider = this.providerFactory.create(getConfig());
        this.isSessionProviderAdded = true;
    }

    private Map<String, String> getConfig()
            throws IOException
    {
        Map<String, String> result;
        if (SESSION_PROPERTY_PROVIDER_CONFIG.exists()) {
            result = loadProperties(SESSION_PROPERTY_PROVIDER_CONFIG);
        }
        else {
            result = ImmutableMap.of();
        }
        return result;
    }

    public SystemSessionPropertyProvider getNotificationProvider()
    {
        return this.provider;
    }
}
