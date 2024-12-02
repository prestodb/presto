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
import com.facebook.presto.spi.SchemaTableName;

import java.util.Collection;

public interface IProcedureRegistry
{
    void addProcedures(ConnectorId connectorId, Collection<Procedure> procedures);

    void removeProcedures(ConnectorId connectorId);

    Procedure resolve(ConnectorId connectorId, SchemaTableName name);

    DistributedProcedure resolveDistributed(ConnectorId connectorId, SchemaTableName name);

    boolean isDistributedProcedure(ConnectorId connectorId, SchemaTableName name);
}
