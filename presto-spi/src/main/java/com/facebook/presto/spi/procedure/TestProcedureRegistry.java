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

import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorProcedureContext;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.StandardErrorCode.PROCEDURE_NOT_FOUND;
import static java.util.Objects.requireNonNull;

public class TestProcedureRegistry
        implements IProcedureRegistry
{
    private final Map<ConnectorId, Map<SchemaTableName, Procedure>> connectorProcedures = new ConcurrentHashMap<>();

    @Override
    public void addProcedures(ConnectorId connectorId, Collection<Procedure> procedures)
    {
        requireNonNull(connectorId, "connectorId is null");
        requireNonNull(procedures, "procedures is null");

        Map<SchemaTableName, Procedure> proceduresByName = procedures.stream().collect(Collectors.toMap(
                procedure -> new SchemaTableName(procedure.getSchema(), procedure.getName()),
                Function.identity()));
        if (connectorProcedures.putIfAbsent(connectorId, proceduresByName) != null) {
            throw new IllegalStateException("Procedures already registered for connector: " + connectorId);
        }
    }

    @Override
    public void removeProcedures(ConnectorId connectorId)
    {
        connectorProcedures.remove(connectorId);
    }

    @Override
    public Procedure resolve(ConnectorId connectorId, SchemaTableName name)
    {
        Map<SchemaTableName, Procedure> procedures = connectorProcedures.get(connectorId);
        if (procedures != null) {
            Procedure procedure = procedures.get(name);
            if (procedure != null) {
                return procedure;
            }
        }
        throw new PrestoException(PROCEDURE_NOT_FOUND, "Procedure not registered: " + name);
    }

    @Override
    public DistributedProcedure resolveDistributed(ConnectorId connectorId, SchemaTableName name)
    {
        Map<SchemaTableName, Procedure> procedures = connectorProcedures.get(connectorId);
        if (procedures != null) {
            Procedure procedure = procedures.get(name);
            if (procedure != null && procedure instanceof DistributedProcedure) {
                return (DistributedProcedure) procedure;
            }
        }
        throw new PrestoException(PROCEDURE_NOT_FOUND, "Distributed procedure not registered: " + name);
    }

    @Override
    public boolean isDistributedProcedure(ConnectorId connectorId, SchemaTableName name)
    {
        Map<SchemaTableName, Procedure> procedures = connectorProcedures.get(connectorId);
        return procedures != null &&
                procedures.containsKey(name) &&
                procedures.get(name) instanceof DistributedProcedure;
    }

    public static class TestProcedureContext
            implements ConnectorProcedureContext
    {}
}
