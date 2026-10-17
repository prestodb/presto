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
package com.facebook.presto.spi;

/**
 * Marks the handle a connector returns for a materialized view refresh, as distinct from a plain
 * insert.
 *
 * <p>It extends {@link ConnectorInsertTableHandle} so that existing connectors and every existing
 * caller keep working unchanged; the refresh commit still travels as an insert. The distinction
 * exists so that {@code finishRefreshMaterializedView} can tell a refresh from an insert without
 * inspecting a connector-specific handle.
 *
 * <p>Deliberately empty. An earlier revision also exposed an affected-rows delete handle, intended
 * for connectors that commit removals and recomputed rows in one transaction, but nothing on the
 * planning or execution path consumed it -- a connector implementing it could not have had it
 * honoured. It is left out until that path exists, rather than published as SPI that does nothing.
 */
public interface ConnectorRefreshMaterializedViewHandle
        extends ConnectorInsertTableHandle
{
}
