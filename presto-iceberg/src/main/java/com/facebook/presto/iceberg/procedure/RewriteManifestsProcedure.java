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
package com.facebook.presto.iceberg.procedure;

import com.facebook.presto.iceberg.IcebergAbstractMetadata;
import com.facebook.presto.iceberg.IcebergMetadataFactory;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.procedure.Procedure;
import com.facebook.presto.spi.procedure.Procedure.Argument;
import com.google.common.collect.ImmutableList;
import jakarta.inject.Inject;
import org.apache.iceberg.RewriteManifests;
import org.apache.iceberg.Table;

import javax.inject.Provider;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.common.block.MethodHandleUtil.methodHandle;
import static com.facebook.presto.common.type.StandardTypes.INTEGER;
import static com.facebook.presto.common.type.StandardTypes.VARCHAR;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_INVALID_SPEC_ID;
import static com.facebook.presto.iceberg.IcebergUtil.getIcebergTable;
import static java.util.Objects.requireNonNull;

public class RewriteManifestsProcedure
        implements Provider<Procedure>
{
    private static final MethodHandle REWRITE_MANIFESTS = methodHandle(
            RewriteManifestsProcedure.class,
            "rewriteManifests",
            ConnectorSession.class,
            String.class,
            String.class,
            Integer.class);

    private final IcebergMetadataFactory metadataFactory;

    @Inject
    public RewriteManifestsProcedure(IcebergMetadataFactory metadataFactory)
    {
        this.metadataFactory = requireNonNull(metadataFactory, "metadataFactory is null");
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                "rewrite_manifests",
                ImmutableList.of(
                        new Argument("schema", VARCHAR),
                        new Argument("table_name", VARCHAR),
                        new Argument("spec_id", INTEGER, false, null)),
                REWRITE_MANIFESTS.bindTo(this));
    }

    public void rewriteManifests(ConnectorSession clientSession, String schemaName, String tableName, Integer specId)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            SchemaTableName schemaTableName = new SchemaTableName(schemaName, tableName);
            IcebergAbstractMetadata metadata = (IcebergAbstractMetadata) metadataFactory.create();
            Table icebergTable = getIcebergTable(metadata, clientSession, schemaTableName);
            RewriteManifests rewriteManifests = icebergTable.rewriteManifests().clusterBy(file -> "file");
            int targetSpecId;
            if (specId != null) {
                if (!icebergTable.specs().containsKey(specId)) {
                    throw new PrestoException(ICEBERG_INVALID_SPEC_ID, "Given spec id does not exist: " + specId);
                }
                targetSpecId = specId;
            }
            else {
                targetSpecId = icebergTable.spec().specId();
            }
            rewriteManifests.rewriteIf(manifest -> manifest.partitionSpecId() == targetSpecId).commit();
            metadata.commit();
        }
    }
}
