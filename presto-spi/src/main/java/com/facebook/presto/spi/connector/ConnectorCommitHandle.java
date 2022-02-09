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
package com.facebook.presto.spi.connector;

import com.facebook.presto.spi.ConnectorId;

import java.util.List;
import java.util.Set;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;

public interface ConnectorCommitHandle
{
    default Object getOutputLastDataCommitTimes(String outputKey)
    {
        return emptyList();
    }

    default boolean containOutput(String outputKey)
    {
        return false;
    }

    default void removeOutput(String outputKey)
    {
    }

    default Set<String> getOutputKeys()
    {
        return emptySet();
    }

    default ConnectorId getConnectorId()
    {
        return new ConnectorId("Unknown");
    }
}
