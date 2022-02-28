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
package com.facebook.presto.hive.metastore;

import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.connector.ConnectorCommitHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class HiveCommitHandle
        implements ConnectorCommitHandle
{
    private Map<String, List<DateTime>> outputLastDataCommitTimes;
    private ConnectorId connectorId;

    @JsonCreator
    public HiveCommitHandle(
            @JsonProperty("connectorId") ConnectorId connectorId,
            @JsonProperty("outputLastDataCommitTimes") Map<String, List<DateTime>> outputLastDataCommitTimes)
    {
        this.connectorId = requireNonNull(connectorId);
        this.outputLastDataCommitTimes = requireNonNull(outputLastDataCommitTimes);
    }

    public ConnectorId getConnectorId()
    {
        return connectorId;
    }

    public void addOutputLastDataCommitTimes(String key, List<DateTime> value)
    {
        if (!outputLastDataCommitTimes.containsKey(key)) {
            outputLastDataCommitTimes.put(key, new ArrayList<DateTime>());
        }
        outputLastDataCommitTimes.get(key).addAll(value);
    }

    @JsonProperty
    public List<DateTime> getOutputLastDataCommitTimes(String key)
    {
        return outputLastDataCommitTimes.get(key);
    }

    public boolean containOutput(String key)
    {
        return outputLastDataCommitTimes.containsKey(key);
    }

    public Set<String> getOutputKeys()
    {
        return outputLastDataCommitTimes.keySet();
    }

    public void removeOutput(String key)
    {
        outputLastDataCommitTimes.remove(key);
    }
}
