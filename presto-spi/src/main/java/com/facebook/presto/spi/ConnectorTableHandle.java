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

public interface ConnectorTableHandle
{
    /**
     * Returns a copy of this handle marked so that filters derived from it
     * during planning are used only for coordinator-side file/split selection
     * and must NOT be applied as row-level filters by workers. The default
     * no-op is safe for connectors that do not support this distinction.
     *
     * @return a handle with the file-selection-only flag set, or {@code this}
     *         if the flag is already set or the connector does not support it
     */
    default ConnectorTableHandle withFilterForFileSelectionOnly()
    {
        return this;
    }
}
