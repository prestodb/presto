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
package io.prestosql.plugin.resourcegroups.db;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import io.airlift.configuration.Config;
import org.weakref.jmx.ObjectNameBuilder;
import org.weakref.jmx.ObjectNameGenerator;

import java.util.Map;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class PrefixObjectNameGeneratorModule
        implements Module
{
    private static final String CONNECTOR_PACKAGE_NAME = "io.prestosql.plugin.resourcegroups.db";
    private static final String DEFAULT_DOMAIN_BASE = "presto.plugin.resourcegroups.db";

    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(PrefixObjectNameGeneratorConfig.class);
    }

    @Provides
    ObjectNameGenerator createPrefixObjectNameGenerator(PrefixObjectNameGeneratorConfig config)
    {
        String domainBase = DEFAULT_DOMAIN_BASE;
        if (config.getDomainBase() != null) {
            domainBase = config.getDomainBase();
        }
        return new PrefixObjectNameGenerator(domainBase);
    }

    public static class PrefixObjectNameGeneratorConfig
    {
        private String domainBase;

        public String getDomainBase()
        {
            return domainBase;
        }

        @Config("jmx.base-name")
        public PrefixObjectNameGeneratorConfig setDomainBase(String domainBase)
        {
            this.domainBase = domainBase;
            return this;
        }
    }

    public static final class PrefixObjectNameGenerator
            implements ObjectNameGenerator
    {
        private final String domainBase;

        public PrefixObjectNameGenerator(String domainBase)
        {
            this.domainBase = domainBase;
        }

        @Override
        public String generatedNameOf(Class<?> type, Map<String, String> properties)
        {
            return new ObjectNameBuilder(toDomain(type))
                    .withProperties(properties)
                    .build();
        }

        private String toDomain(Class<?> type)
        {
            String domain = type.getPackage().getName();
            if (domain.startsWith(CONNECTOR_PACKAGE_NAME)) {
                domain = domainBase + domain.substring(CONNECTOR_PACKAGE_NAME.length());
            }
            return domain;
        }
    }
}
