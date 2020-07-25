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
import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.spi.function.FunctionImplementationType;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import java.util.Map;

import static com.facebook.airlift.json.JsonCodec.mapJsonCodec;
import static com.facebook.presto.spi.function.RoutineCharacteristics.Language;
import static com.facebook.presto.spi.function.RoutineCharacteristics.Language.SQL;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;

public class SqlInvokedFunctionNamespaceManagerConfig
{
    private static final JsonCodec<Map<Language, FunctionImplementationType>> FUNCTION_LANGUAGES_CODEC = mapJsonCodec(Language.class, FunctionImplementationType.class);

    private Duration functionCacheExpiration = new Duration(5, MINUTES);
    private Duration functionInstanceCacheExpiration = new Duration(8, HOURS);
    private Map<Language, FunctionImplementationType> supportedFunctionLanguages = ImmutableMap.of(SQL, FunctionImplementationType.SQL);

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

    public Map<Language, FunctionImplementationType> getSupportedFunctionLanguages()
    {
        return supportedFunctionLanguages;
    }

    @Config("supported-function-languages")
    public SqlInvokedFunctionNamespaceManagerConfig setSupportedFunctionLanguages(String languages)
    {
        this.supportedFunctionLanguages = FUNCTION_LANGUAGES_CODEC.fromJson(languages);
        return this;
    }
}
