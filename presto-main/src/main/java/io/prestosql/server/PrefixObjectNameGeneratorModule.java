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
package io.prestosql.server;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import org.weakref.jmx.ObjectNameBuilder;
import org.weakref.jmx.ObjectNameGenerator;

import java.util.Map;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

/**
 * Replaces the prefix given by {@code packagePrefix} with the domain base provided via configuration.
 */
public class PrefixObjectNameGeneratorModule
        implements Module
{
    private final String packagePrefix;

    public PrefixObjectNameGeneratorModule(String packagePrefix)
    {
        this.packagePrefix = requireNonNull(packagePrefix, "packagePrefix is null");
    }

    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(JmxNamingConfig.class);
    }

    @Provides
    ObjectNameGenerator createPrefixObjectNameGenerator(JmxNamingConfig jmxNamingConfig)
    {
        return new PrefixObjectNameGenerator(packagePrefix, jmxNamingConfig.getDomainBase());
    }

    public static final class PrefixObjectNameGenerator
            implements ObjectNameGenerator
    {
        private final String packagePrefix;
        private final String replacement;

        public PrefixObjectNameGenerator(String packagePrefix, String replacement)
        {
            this.packagePrefix = packagePrefix;
            this.replacement = replacement;
        }

        @Override
        public String generatedNameOf(Class<?> type, Map<String, String> properties)
        {
            String domain = type.getPackage().getName();
            if (domain.startsWith(packagePrefix)) {
                domain = replacement + domain.substring(packagePrefix.length());
            }

            return new ObjectNameBuilder(domain)
                    .withProperties(properties)
                    .build();
        }
    }
}
