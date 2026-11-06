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
 * Opt-in marker: the connector implements the refresh-specific
 * {@code ConnectorMetadata.finishRefreshMaterializedView} overloads that take this type, rather than
 * only the plain-insert finish.
 *
 * <p>It extends {@link ConnectorInsertTableHandle} because a refresh commits as an insert, which also
 * lets existing connectors and callers keep working unchanged.
 *
 * <p>Implement it only on a handle returned <em>exclusively</em> from the refresh path. A connector
 * that puts it on the class it also returns from {@code beginInsert} makes {@code instanceof} true
 * for every ordinary insert, so the type stops carrying information; such a connector should leave
 * it unimplemented and receive the {@link ConnectorInsertTableHandle} overloads instead. The Iceberg
 * connector is in that position -- one handle class serves both paths -- and so does not implement
 * this.
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
