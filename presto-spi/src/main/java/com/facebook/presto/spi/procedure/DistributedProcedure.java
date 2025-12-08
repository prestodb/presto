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

    public abstract ConnectorDistributedProcedureHandle begin(ConnectorSession session, ConnectorProcedureContext procedureContext, ConnectorTableLayoutHandle tableLayoutHandle, Object[] arguments);

    public abstract void finish(ConnectorSession session, ConnectorProcedureContext procedureContext, ConnectorDistributedProcedureHandle procedureHandle, Collection<Slice> fragments);

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
