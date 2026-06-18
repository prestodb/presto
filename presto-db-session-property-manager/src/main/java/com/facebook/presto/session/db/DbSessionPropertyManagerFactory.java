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
package com.facebook.presto.session.db;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.json.JsonModule;
import com.facebook.presto.spi.resourceGroups.SessionPropertyConfigurationManagerContext;
import com.facebook.presto.spi.session.SessionPropertyConfigurationManager;
import com.facebook.presto.spi.session.SessionPropertyConfigurationManagerFactory;
import com.google.inject.Injector;

import java.util.Map;

import static com.google.common.base.Throwables.throwIfUnchecked;

public class DbSessionPropertyManagerFactory
        implements SessionPropertyConfigurationManagerFactory
{
    @Override
    public String getName()
    {
        return "db";
    }

    @Override
    public SessionPropertyConfigurationManager create(Map<String, String> config, SessionPropertyConfigurationManagerContext context)
    {
        try {
            Bootstrap app = new Bootstrap(new JsonModule(), new DbSessionPropertyManagerModule());
            Injector injector = app
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();

            return injector.getInstance(DbSessionPropertyManager.class);
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }
}
