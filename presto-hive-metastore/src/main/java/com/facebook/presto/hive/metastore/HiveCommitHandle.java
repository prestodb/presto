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
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HiveCommitHandle
        implements ConnectorCommitHandle
{
    private final Map<String, List<DateTime>> outputLastDataCommitTimes = new HashMap<>();
    private ConnectorId connectorId = new ConnectorId("Unknown");

    public void setConnectorId(ConnectorId id)
    {
        connectorId = id;
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
