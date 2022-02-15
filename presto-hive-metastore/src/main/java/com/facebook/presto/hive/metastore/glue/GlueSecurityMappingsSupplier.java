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
package com.facebook.presto.hive.metastore.glue;

import com.facebook.airlift.log.Logger;
import com.google.common.base.Suppliers;
import io.airlift.units.Duration;

import java.io.File;
import java.util.Optional;
import java.util.function.Supplier;

import static com.facebook.presto.plugin.base.util.JsonUtils.parseJson;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class GlueSecurityMappingsSupplier
{
    private static final Logger log = Logger.get(GlueSecurityMappingsSupplier.class);

    private final Supplier<GlueSecurityMappings> mappingsSupplier;

    public GlueSecurityMappingsSupplier(Optional<File> configFile, Optional<Duration> refreshPeriod)
    {
        this.mappingsSupplier = getMappings(configFile, refreshPeriod);
    }

    private static Supplier<GlueSecurityMappings> getMappings(Optional<File> configFile, Optional<Duration> refreshPeriod)
    {
        if (!configFile.isPresent()) {
            return null;
        }

        Supplier<GlueSecurityMappings> supplier = () -> parseJson(configFile.get().toPath(), GlueSecurityMappings.class);
        return refreshPeriod.<Supplier<GlueSecurityMappings>>map(duration -> Suppliers.memoizeWithExpiration(
                () -> {
                    log.info("Refreshing Glue security mapping configuration from %s", configFile);
                    return supplier.get();
                },
                duration.toMillis(),
                MILLISECONDS)).orElseGet(() -> Suppliers.memoize(supplier::get));
    }

    public Supplier<GlueSecurityMappings> getMappingsSupplier()
    {
        return mappingsSupplier;
    }
}
