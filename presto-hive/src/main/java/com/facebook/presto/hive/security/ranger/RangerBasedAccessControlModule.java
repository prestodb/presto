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

package com.facebook.presto.hive.security.ranger;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.plugin.base.security.ForwardingConnectorAccessControl;
import com.facebook.presto.spi.connector.ConnectorAccessControl;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Provides;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.google.common.base.Suppliers.memoizeWithExpiration;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class RangerBasedAccessControlModule
        implements Module
{
    private static final Logger log = Logger.get(RangerBasedAccessControlModule.class);

    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(RangerBasedAccessControlConfig.class);
    }

    @Inject
    @Provides
    public ConnectorAccessControl getConnectorAccessControl(RangerBasedAccessControlConfig config)
    {
        if (config.getRefreshPeriod() != null) {
            return ForwardingConnectorAccessControl.of(memoizeWithExpiration(
                    () -> {
                        return new RangerBasedAccessControl(config);
                    },
                    config.getRefreshPeriod().toMillis(),
                    MILLISECONDS));
        }
        return new RangerBasedAccessControl(config);
    }
}
