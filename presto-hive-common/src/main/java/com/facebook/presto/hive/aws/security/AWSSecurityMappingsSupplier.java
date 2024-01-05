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
package com.facebook.presto.hive.aws.security;

import com.facebook.airlift.log.Logger;
import com.google.common.base.Suppliers;
import io.airlift.units.Duration;

import java.io.File;
import java.util.Optional;
import java.util.function.Supplier;

import static com.facebook.presto.plugin.base.JsonUtils.parseJson;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class AWSSecurityMappingsSupplier
{
    private static final Logger log = Logger.get(AWSSecurityMappingsSupplier.class);
    private final Supplier<AWSSecurityMappings> mappingsSupplier;

    public AWSSecurityMappingsSupplier(Optional<File> configFile, Optional<Duration> refreshPeriod)
    {
        this.mappingsSupplier = getMappings(configFile, refreshPeriod);
    }

    private static Supplier<AWSSecurityMappings> getMappings(Optional<File> configFile, Optional<Duration> refreshPeriod)
    {
        if (!configFile.isPresent()) {
            return null;
        }

        Supplier<AWSSecurityMappings> supplier = () -> parseJson(configFile.get().toPath(), AWSSecurityMappings.class);
        return refreshPeriod.<Supplier<AWSSecurityMappings>>map(duration -> Suppliers.memoizeWithExpiration(
                () -> {
                    log.info("Refreshing AWS security mapping configuration from %s", configFile);
                    return supplier.get();
                },
                duration.toMillis(),
                MILLISECONDS)).orElseGet(() -> Suppliers.memoize(supplier::get));
    }

    public Supplier<AWSSecurityMappings> getMappingsSupplier()
    {
        return mappingsSupplier;
    }
}
