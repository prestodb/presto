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
package com.facebook.presto.plugin.turbonium;

import com.facebook.presto.plugin.turbonium.config.TurboniumConfigManager;
import com.facebook.presto.plugin.turbonium.config.UpdateDisableEncodingProcedure;
import com.facebook.presto.plugin.turbonium.config.UpdateMaxDataPerNodeProcedure;
import com.facebook.presto.plugin.turbonium.config.UpdateMaxTableSizePerNodeProcedure;
import com.facebook.presto.plugin.turbonium.config.UpdateSplitsPerNodeProcedure;
import com.facebook.presto.plugin.turbonium.systemtables.TurboniumColumnStatsSystemTable;
import com.facebook.presto.plugin.turbonium.systemtables.TurboniumConfigSystemTable;
import com.facebook.presto.plugin.turbonium.systemtables.TurboniumTableIdSystemTable;
import com.facebook.presto.plugin.turbonium.systemtables.TurboniumTableStatsSystemTable;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.procedure.Procedure;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.multibindings.MultibindingsScanner;
import com.google.inject.multibindings.ProvidesIntoSet;

import javax.inject.Inject;

import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class TurboniumModule
        implements Module
{
    private final String connectorId;
    private final TypeManager typeManager;
    private final NodeManager nodeManager;

    public TurboniumModule(String connectorId, TypeManager typeManager, NodeManager nodeManager)
    {
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
    }

    @Override
    public void configure(Binder binder)
    {
        binder.install(MultibindingsScanner.asModule());
        binder.bind(TypeManager.class).toInstance(typeManager);
        binder.bind(NodeManager.class).toInstance(nodeManager);
        binder.bind(TurboniumConnector.class).in(Scopes.SINGLETON);
        binder.bind(TurboniumConnectorId.class).toInstance(new TurboniumConnectorId(connectorId));
        binder.bind(TurboniumMetadata.class).in(Scopes.SINGLETON);
        binder.bind(TurboniumSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(TurboniumPagesStore.class).in(Scopes.SINGLETON);
        binder.bind(TurboniumPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(TurboniumPageSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(TurboniumNodePartitioningProvider.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(TurboniumConfig.class);
        binder.bind(TurboniumConfigManager.class).in(Scopes.SINGLETON);
        Multibinder<SystemTable> tableBinder = newSetBinder(binder, SystemTable.class);
        tableBinder.addBinding().to(TurboniumTableStatsSystemTable.class).in(Scopes.SINGLETON);
        tableBinder.addBinding().to(TurboniumColumnStatsSystemTable.class).in(Scopes.SINGLETON);
        tableBinder.addBinding().to(TurboniumConfigSystemTable.class).in(Scopes.SINGLETON);
        tableBinder.addBinding().to(TurboniumTableIdSystemTable.class).in(Scopes.SINGLETON);
        Multibinder.newSetBinder(binder, Procedure.class);
        binder.bind(UpdateMaxDataPerNodeProcedure.class).in(Scopes.SINGLETON);
        binder.bind(UpdateMaxTableSizePerNodeProcedure.class).in(Scopes.SINGLETON);
        binder.bind(UpdateSplitsPerNodeProcedure.class).in(Scopes.SINGLETON);
        binder.bind(UpdateDisableEncodingProcedure.class).in(Scopes.SINGLETON);
    }

    @ProvidesIntoSet
    public static Procedure getUpdateMaxDataPerNodeProcedure(UpdateMaxDataPerNodeProcedure procedure)
    {
        return procedure.getProcedure();
    }

    @ProvidesIntoSet
    public static Procedure getUpdateMaxTableSizePerNodeProcedure(UpdateMaxTableSizePerNodeProcedure procedure)
    {
        return procedure.getProcedure();
    }

    @ProvidesIntoSet
    public static Procedure getUpdateSplitsPerNodeProcedure(UpdateSplitsPerNodeProcedure procedure)
    {
        return procedure.getProcedure();
    }

    @ProvidesIntoSet
    public static Procedure getUpdateDisableEncodingProcedure(UpdateDisableEncodingProcedure procedure)
    {
        return procedure.getProcedure();
    }

    public static final class TypeDeserializer
            extends FromStringDeserializer<Type>
    {
        private final TypeManager typeManager;

        @Inject
        public TypeDeserializer(TypeManager typeManager)
        {
            super(Type.class);
            this.typeManager = requireNonNull(typeManager, "typeManager is null");
        }

        @Override
        protected Type _deserialize(String value, DeserializationContext context)
        {
            Type type = typeManager.getType(parseTypeSignature(value));
            checkArgument(type != null, "Unknown type %s", value);
            return type;
        }
    }
}
