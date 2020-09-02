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
package com.facebook.presto.verifier.source;

import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.facebook.presto.verifier.framework.VerifierConfig;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;

import java.util.Set;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.presto.verifier.source.MySqlSourceQuerySupplier.MYSQL_SOURCE_QUERY_SUPPLIER;
import static com.facebook.presto.verifier.source.PrestoQuerySourceQuerySupplier.PRESTO_QUERY_SOURCE_QUERY_SUPPLIER;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.inject.Scopes.SINGLETON;

public class SourceQueryModule
        extends AbstractConfigurationAwareModule
{
    private static final String SOURCE_QUERY_CONFIG_PREFIX = "source-query";
    private final Set<String> supportedSourceQuerySupplierTypes;

    public SourceQueryModule(Set<String> customSourceQuerySupplierTypes)
    {
        this.supportedSourceQuerySupplierTypes = ImmutableSet.<String>builder()
                .add(MYSQL_SOURCE_QUERY_SUPPLIER)
                .add(PRESTO_QUERY_SOURCE_QUERY_SUPPLIER)
                .addAll(customSourceQuerySupplierTypes)
                .build();
    }

    @Override
    protected void setup(Binder binder)
    {
        String sourceQuerySupplierType = buildConfigObject(VerifierConfig.class).getSourceQuerySupplier();
        checkArgument(supportedSourceQuerySupplierTypes.contains(sourceQuerySupplierType), "Unsupported SourceQuerySupplier: %s", sourceQuerySupplierType);

        if (MYSQL_SOURCE_QUERY_SUPPLIER.equals(sourceQuerySupplierType)) {
            configBinder(binder).bindConfig(MySqlSourceQueryConfig.class, SOURCE_QUERY_CONFIG_PREFIX);
            binder.bind(SourceQuerySupplier.class).to(MySqlSourceQuerySupplier.class).in(SINGLETON);
        }

        if (PRESTO_QUERY_SOURCE_QUERY_SUPPLIER.equals(sourceQuerySupplierType)) {
            configBinder(binder).bindConfig(PrestoQuerySourceQueryConfig.class, SOURCE_QUERY_CONFIG_PREFIX);
            binder.bind(SourceQuerySupplier.class).to(PrestoQuerySourceQuerySupplier.class).in(SINGLETON);
        }
    }
}
