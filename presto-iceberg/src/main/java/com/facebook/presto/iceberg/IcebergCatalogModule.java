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
package com.facebook.presto.iceberg;

import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.iceberg.hadoop.IcebergHadoopCatalogModule;
import com.facebook.presto.iceberg.nessie.IcebergNessieCatalogModule;
import com.facebook.presto.iceberg.rest.IcebergRestCatalogModule;
import com.google.inject.Binder;
import com.google.inject.Module;

import java.util.Optional;

import static com.facebook.airlift.configuration.ConditionalModule.installModuleIf;
import static com.facebook.presto.iceberg.CatalogType.HADOOP;
import static com.facebook.presto.iceberg.CatalogType.HIVE;
import static com.facebook.presto.iceberg.CatalogType.NESSIE;
import static com.facebook.presto.iceberg.CatalogType.REST;

public class IcebergCatalogModule
        extends AbstractConfigurationAwareModule
{
    private final String connectorId;
    private final Optional<ExtendedHiveMetastore> metastore;

    public IcebergCatalogModule(String connectorId, Optional<ExtendedHiveMetastore> metastore)
    {
        this.connectorId = connectorId;
        this.metastore = metastore;
    }

    @Override
    protected void setup(Binder binder)
    {
        bindCatalogModule(HIVE, new IcebergHiveModule(connectorId, metastore));
        bindCatalogModule(HADOOP, new IcebergHadoopCatalogModule());
        bindCatalogModule(NESSIE, new IcebergNessieCatalogModule());
        bindCatalogModule(REST, new IcebergRestCatalogModule());
    }

    private void bindCatalogModule(CatalogType catalogType, Module module)
    {
        install(installModuleIf(
                IcebergConfig.class,
                config -> config.getCatalogType().equals(catalogType),
                module));
    }
}
