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
package com.facebook.presto.sidecar.sessionpropertyproviders;

import com.facebook.airlift.configuration.Config;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import static java.util.concurrent.TimeUnit.MINUTES;

public class NativeSystemSessionPropertyProviderConfig
{
    private Duration sessionPropertiesCacheExpiration = new Duration(5, MINUTES);

    @MinDuration("0ns")
    public Duration getSessionPropertiesCacheExpiration()
    {
        return sessionPropertiesCacheExpiration;
    }

    @Config("session-properties-cache-expiration")
    public NativeSystemSessionPropertyProviderConfig setSessionPropertiesCacheExpiration(Duration functionCacheExpiration)
    {
        this.sessionPropertiesCacheExpiration = functionCacheExpiration;
        return this;
    }
}
