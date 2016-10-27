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
package com.facebook.presto.metadata;

import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.procedure.Procedure;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.Maps;
import com.google.common.primitives.Primitives;

import javax.annotation.concurrent.ThreadSafe;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.facebook.presto.spi.StandardErrorCode.PROCEDURE_NOT_FOUND;
import static com.facebook.presto.spi.procedure.Procedure.Argument;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.StandardTypes.ARRAY;
import static com.facebook.presto.spi.type.StandardTypes.MAP;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

@ThreadSafe
public class ProcedureRegistry
{
    private final Map<ConnectorId, Map<SchemaTableName, Procedure>> connectorProcedures = new ConcurrentHashMap<>();

    public void addProcedures(ConnectorId connectorId, Collection<Procedure> procedures)
    {
        requireNonNull(connectorId, "connectorId is null");
        requireNonNull(procedures, "procedures is null");

        procedures.forEach(ProcedureRegistry::validateProcedure);

        Map<SchemaTableName, Procedure> proceduresByName = Maps.uniqueIndex(
                procedures,
                procedure -> new SchemaTableName(procedure.getSchema(), procedure.getName()));

        checkState(connectorProcedures.putIfAbsent(connectorId, proceduresByName) == null, "Procedures already registered for connector: %s", connectorId);
    }

    public void removeProcedures(ConnectorId connectorId)
    {
        connectorProcedures.remove(connectorId);
    }

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

    private static void validateProcedure(Procedure procedure)
    {
        List<Class<?>> parameters = procedure.getMethodHandle().type().parameterList().stream()
                .filter(type -> !ConnectorSession.class.isAssignableFrom(type))
                .collect(toList());
        for (int i = 0; i < procedure.getArguments().size(); i++) {
            Argument argument = procedure.getArguments().get(i);
            Class<?> argumentType = Primitives.unwrap(parameters.get(i));
            Class<?> expectedType = getObjectType(argument.getType());
            checkArgument(expectedType.equals(argumentType),
                    "Argument '%s' has invalid type %s (expected %s)",
                    argument.getName(),
                    argumentType.getName(),
                    expectedType.getName());
        }
    }

    private static Class<?> getObjectType(Type type)
    {
        if (type.equals(BOOLEAN)) {
            return boolean.class;
        }
        if (type.equals(BIGINT)) {
            return long.class;
        }
        if (type.equals(DOUBLE)) {
            return double.class;
        }
        if (type.equals(VARCHAR)) {
            return String.class;
        }
        if (type.getTypeSignature().getBase().equals(ARRAY)) {
            getObjectType(type.getTypeParameters().get(0));
            return List.class;
        }
        if (type.getTypeSignature().getBase().equals(MAP)) {
            getObjectType(type.getTypeParameters().get(0));
            getObjectType(type.getTypeParameters().get(1));
            return Map.class;
        }
        throw new IllegalArgumentException("Unsupported argument type: " + type.getDisplayName());
    }
}
