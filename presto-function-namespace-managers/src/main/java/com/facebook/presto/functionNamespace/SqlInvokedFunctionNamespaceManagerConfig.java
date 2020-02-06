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
package com.facebook.presto.functionNamespace;

import com.facebook.airlift.configuration.Config;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;

public class SqlInvokedFunctionNamespaceManagerConfig
{
    private Duration functionCacheExpiration = new Duration(5, MINUTES);
    private Duration functionInstanceCacheExpiration = new Duration(8, HOURS);

    @MinDuration("0ns")
    public Duration getFunctionCacheExpiration()
    {
        return functionCacheExpiration;
    }

    @Config("function-cache-expiration")
    public SqlInvokedFunctionNamespaceManagerConfig setFunctionCacheExpiration(Duration functionCacheExpiration)
    {
        this.functionCacheExpiration = functionCacheExpiration;
        return this;
    }

    @MinDuration("0ns")
    public Duration getFunctionInstanceCacheExpiration()
    {
        return functionInstanceCacheExpiration;
    }

    @Config("function-instance-cache-expiration")
    public SqlInvokedFunctionNamespaceManagerConfig setFunctionInstanceCacheExpiration(Duration functionInstanceCacheExpiration)
    {
        this.functionInstanceCacheExpiration = functionInstanceCacheExpiration;
        return this;
    }
}
