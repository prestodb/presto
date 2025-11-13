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

import com.facebook.presto.spi.connector.ConnectorMetadata;

public interface ConnectorMergeTableHandle
{
    /**
     * This method is required because the {@link ConnectorTableHandle} returned by
     * {@link ConnectorMetadata#beginMerge} is in general different from the
     * one passed to that method, but the updated handle must be made
     * available to {@link ConnectorMetadata#finishMerge}
     *
     * @return the {@link ConnectorTableHandle} returned by {@link ConnectorMetadata#beginMerge}
     */
    ConnectorTableHandle getTableHandle();
}
