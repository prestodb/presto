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

import com.facebook.presto.spi.ConnectorDistributedProcedureHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.connector.ConnectorProcedureContext;
import io.airlift.slice.Slice;

import java.util.Collection;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class DistributedProcedure
        extends Procedure
{
    private final DistributedProcedureType type;
    private final BeginCallDistributedProcedure beginCallDistributedProcedure;
    private final FinishCallDistributedProcedure finishCallDistributedProcedure;

    protected DistributedProcedure(DistributedProcedureType type, String schema, String name, List<Argument> arguments, BeginCallDistributedProcedure beginCallDistributedProcedure, FinishCallDistributedProcedure finishCallDistributedProcedure)
    {
        super(schema, name, arguments);
        this.type = requireNonNull(type, "distributed procedure type is null");
        this.beginCallDistributedProcedure = requireNonNull(beginCallDistributedProcedure, "beginCallDistributedProcedure is null");
        this.finishCallDistributedProcedure = requireNonNull(finishCallDistributedProcedure, "finishCallDistributedProcedure is null");
    }

    public DistributedProcedureType getType()
    {
        return type;
    }

    public BeginCallDistributedProcedure getBeginCallDistributedProcedure()
    {
        return beginCallDistributedProcedure;
    }

    public FinishCallDistributedProcedure getFinishCallDistributedProcedure()
    {
        return finishCallDistributedProcedure;
    }

    public ConnectorProcedureContext createContext()
    {
        throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "createContext not supported");
    }

    public enum DistributedProcedureType
    {
        TABLE_DATA_REWRITE
    }

    @FunctionalInterface
    public interface BeginCallDistributedProcedure
    {
        ConnectorDistributedProcedureHandle begin(ConnectorSession session, ConnectorProcedureContext procedureContext, ConnectorTableLayoutHandle tableLayoutHandle, Object[] arguments);
    }

    @FunctionalInterface
    public interface FinishCallDistributedProcedure
    {
        void finish(ConnectorProcedureContext procedureContext, ConnectorDistributedProcedureHandle procedureHandle, Collection<Slice> fragments);
    }
}
