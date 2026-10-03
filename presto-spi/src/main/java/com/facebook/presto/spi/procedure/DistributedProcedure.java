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
package com.facebook.presto.spi.procedure;

import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.spi.ConnectorDistributedProcedureHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.connector.ConnectorProcedureContext;
import io.airlift.slice.Slice;
import jakarta.annotation.Nullable;

import java.util.Collection;
import java.util.List;

import static java.util.Objects.requireNonNull;

public abstract class DistributedProcedure
        extends BaseProcedure<DistributedProcedure.Argument>
{
    private final DistributedProcedureType type;

    protected DistributedProcedure(DistributedProcedureType type, String schema, String name, List<Argument> arguments)
    {
        super(schema, name, arguments);
        this.type = requireNonNull(type, "distributed procedure type is null");
    }

    public DistributedProcedureType getType()
    {
        return type;
    }

    /**
     *Indicates whether this distributed procedure should execute using the procedure's own catalog
     * as the effective catalog for internal execution.
     *
     * When this method returns (@code true), the procedure will override the catalog from the caller's
     * session and execute using the procedure's associated catalog as the default catalog. This ensures
     * that table resolution and metadata access are scoped to the procedure's catalog, rather than the
     * catalog of the initiating session. This behavior is typically required for catalog-specific
     * procedures (e.g., Iceberg maintenance procedure such as `rewrite_data_files`).
     *
     * When this method returns {@code false}, the procedure does not override the session catalog and
     * instead executes in a catalog-agnostic manner. Table resolution follows the caller's session context
     * or fully qualified identifiers, enabling cross-catalog access and federation. This mode is intended
     * for generalized distributed procedures.
     *
     * @return true if the procedure must execute with its own catalog as the effective execution catalog;
     *         false if it should respect the caller's session catalog and support cross-catalog execution
     * */
    public boolean usesProcedureCatalogAsSession()
    {
        return false;
    }

    /**
     * Performs the preparatory work required when starting the execution of this distributed procedure.
     * */
    public abstract ConnectorDistributedProcedureHandle begin(ConnectorSession session, ConnectorProcedureContext procedureContext, ConnectorTableLayoutHandle tableLayoutHandle, Object[] arguments);

    /**
     * Performs the work required for the final centralized commit, after all distributed execution tasks have completed.
     * */
    public abstract void finish(ConnectorSession session, ConnectorProcedureContext procedureContext, ConnectorDistributedProcedureHandle procedureHandle, Collection<Slice> fragments);

    /**
     * Creates a connector-specific, or even a distributed procedure subtype-specific context object.
     * For connectors that support distributed procedures, this method is invoked at the start of a distributed procedure's execution.
     * The generated procedure context is then bound to the current ConnectorMetadata, maintaining all contextual information
     * throughout the execution. This context would be accessed during calls to the procedure's {@link #begin} and {@link #finish} methods.
     */
    public ConnectorProcedureContext createContext(Object... arguments)
    {
        throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "createContext not supported");
    }

    public enum DistributedProcedureType
    {
        TABLE_DATA_REWRITE
    }

    public static class Argument
            extends BaseProcedure.BaseArgument
    {
        public Argument(String name, String type)
        {
            super(name, type);
        }

        public Argument(String name, String type, boolean required, @Nullable Object defaultValue)
        {
            super(name, type, required, defaultValue);
        }

        public Argument(String name, TypeSignature type, boolean required, @Nullable Object defaultValue)
        {
            super(name, type, required, defaultValue);
        }
    }
}
