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

import com.facebook.presto.common.plan.PlanCanonicalizationStrategy;

import java.util.Optional;

public interface ConnectorTableLayoutHandle
{
    /**
     * Identifier is used to identify if the table layout is providing the same set of data.
     *
     * Split is used to update the identifier based on runtime information.
     *
     * Returns an identifier with consistent representation according to canonicalizationStrategy
     */
    default Object getIdentifier(Optional<ConnectorSplit> split, PlanCanonicalizationStrategy canonicalizationStrategy)
    {
        return this;
    }
}
