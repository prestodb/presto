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
package io.prestosql.plugin.base.security;

import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Provides;
import io.airlift.log.Logger;
import io.prestosql.spi.connector.ConnectorAccessControl;

import static com.google.common.base.Suppliers.memoizeWithExpiration;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class FileBasedAccessControlModule
        implements Module
{
    private static final Logger log = Logger.get(FileBasedAccessControlModule.class);

    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(FileBasedAccessControlConfig.class);
    }

    @Inject
    @Provides
    public ConnectorAccessControl getConnectorAccessControl(FileBasedAccessControlConfig config)
    {
        if (config.getRefreshPeriod() != null) {
            return ForwardingConnectorAccessControl.of(memoizeWithExpiration(
                    () -> {
                        log.info("Refreshing system access control from %s", config.getConfigFile());
                        return new FileBasedAccessControl(config);
                    },
                    config.getRefreshPeriod().toMillis(),
                    MILLISECONDS));
        }
        return new FileBasedAccessControl(config);
    }
}
