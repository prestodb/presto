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
package com.facebook.presto.hive.metastore;

import com.facebook.presto.hive.HiveColumnConverterProvider;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.procedure.Procedure;
import com.facebook.presto.spi.procedure.Procedure.Argument;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;
import javax.inject.Provider;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.block.MethodHandleUtil.methodHandle;
import static com.facebook.presto.common.type.StandardTypes.VARCHAR;
import static com.facebook.presto.hive.metastore.MetastoreUtil.getMetastoreHeaders;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class InvalidateMetastoreCacheProcedure
        implements Provider<Procedure>
{
    private static final MethodHandle INVALIDATE_METASTORE_CACHE = methodHandle(
            InvalidateMetastoreCacheProcedure.class,
            "invalidateMetastoreCache",
            ConnectorSession.class,
            String.class,
            String.class,
            List.class,
            List.class);

    private final Optional<InMemoryCachingHiveMetastore> inMemoryCachingHiveMetastore;

    @Inject
    public InvalidateMetastoreCacheProcedure(ExtendedHiveMetastore extendedHiveMetastore)
    {
        requireNonNull(extendedHiveMetastore, "extendedHiveMetastore is null");
        if (extendedHiveMetastore instanceof InMemoryCachingHiveMetastore) {
            this.inMemoryCachingHiveMetastore = Optional.of((InMemoryCachingHiveMetastore) extendedHiveMetastore);
        }
        else {
            this.inMemoryCachingHiveMetastore = Optional.empty();
        }
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                "invalidate_metastore_cache",
                ImmutableList.of(
                        new Argument("schema_name", VARCHAR, false, null),
                        new Argument("table_name", VARCHAR, false, null),
                        new Argument("partition_columns", "array(varchar)", false, null),
                        new Argument("partition_values", "array(varchar)", false, null)),
                INVALIDATE_METASTORE_CACHE.bindTo(this));
    }

    public void invalidateMetastoreCache(ConnectorSession session, String schemaName, String tableName, List<String> partitionColumnNames, List<String> partitionValues)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            doInvalidateMetastoreCache(
                    session,
                    Optional.ofNullable(schemaName),
                    Optional.ofNullable(tableName),
                    Optional.ofNullable(partitionColumnNames).orElse(ImmutableList.of()),
                    Optional.ofNullable(partitionValues).orElse(ImmutableList.of()));
        }
    }

    private void doInvalidateMetastoreCache(
            ConnectorSession session,
            Optional<String> schemaName,
            Optional<String> tableName,
            List<String> partitionColumnNames,
            List<String> partitionValues)
    {
        if (!inMemoryCachingHiveMetastore.isPresent()) {
            throw new PrestoException(NOT_SUPPORTED, "Metastore Cache is not enabled");
        }

        MetastoreContext metastoreContext = new MetastoreContext(
                session.getIdentity(),
                session.getQueryId(),
                session.getClientInfo(),
                session.getClientTags(),
                session.getSource(),
                getMetastoreHeaders(session),
                false,
                HiveColumnConverterProvider.DEFAULT_COLUMN_CONVERTER_PROVIDER,
                session.getWarningCollector(),
                session.getRuntimeStats());

        checkArgument(
                partitionColumnNames.size() == partitionValues.size(),
                "Procedure parameters partition_columns and partition_values should be of same length");

        if (!schemaName.isPresent() && !tableName.isPresent() && partitionColumnNames.isEmpty()) {
            inMemoryCachingHiveMetastore.get().invalidateAll();
        }
        else if (schemaName.isPresent() && !tableName.isPresent() && partitionColumnNames.isEmpty()) {
            inMemoryCachingHiveMetastore.get().invalidateCache(metastoreContext, schemaName.get());
        }
        else if (schemaName.isPresent() && tableName.isPresent() && partitionColumnNames.isEmpty()) {
            inMemoryCachingHiveMetastore.get().invalidateCache(metastoreContext, schemaName.get(), tableName.get());
        }
        else if (schemaName.isPresent() && tableName.isPresent()) {
            inMemoryCachingHiveMetastore.get().invalidateCache(
                    metastoreContext,
                    schemaName.get(),
                    tableName.get(),
                    partitionColumnNames,
                    partitionValues);
        }
        else {
            throw new PrestoException(INVALID_PROCEDURE_ARGUMENT, "Invalid parameters passed");
        }
    }
}
