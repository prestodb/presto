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
package com.facebook.presto.iceberg.rest;

import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.facebook.presto.iceberg.IcebergMetadataFactory;
import com.facebook.presto.iceberg.IcebergNativeCatalogFactory;
import com.facebook.presto.iceberg.IcebergNativeMetadataFactory;
import com.google.inject.Binder;
import com.google.inject.Scopes;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;

public class IcebergRestCatalogModule
        extends AbstractConfigurationAwareModule
{
    @Override
    public void setup(Binder binder)
    {
        configBinder(binder).bindConfig(IcebergRestConfig.class);

        binder.bind(IcebergNativeCatalogFactory.class).to(IcebergRestCatalogFactory.class).in(Scopes.SINGLETON);
        binder.bind(IcebergMetadataFactory.class).to(IcebergNativeMetadataFactory.class).in(Scopes.SINGLETON);
    }
}
