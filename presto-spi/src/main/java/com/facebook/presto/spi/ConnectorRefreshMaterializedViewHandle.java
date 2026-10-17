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

import java.util.Optional;

/**
 * Optional refresh handle for connectors that atomically replace MV rows.
 *
 * <p>The insert handle remains the primary refresh handle for compatibility. Connectors that
 * support affected-row replacement may additionally expose a delete handle, allowing the
 * connector to commit removals and recomputed rows in one transaction.</p>
 */
public interface ConnectorRefreshMaterializedViewHandle
        extends ConnectorInsertTableHandle
{
    default Optional<ConnectorDeleteTableHandle> getAffectedRowsDeleteHandle()
    {
        return Optional.empty();
    }
}
