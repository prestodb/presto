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
package com.facebook.presto.execution;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.facebook.presto.server.thrift.Any;
import com.facebook.presto.spi.ConnectorMetadataUpdateHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

@ThriftStruct
public class TaskWithConnectorType
{
    private final int val;
    //Connector specific Type
    private ConnectorMetadataUpdateHandle connectorMetadataUpdateHandle;
    //Connector specific Type serialized as Any
    private Any connectorMetadataUpdateHandleAny;

    @JsonCreator
    public TaskWithConnectorType(
            int val,
            ConnectorMetadataUpdateHandle connectorMetadataUpdateHandle)
    {
        this.val = val;
        this.connectorMetadataUpdateHandle = connectorMetadataUpdateHandle;
    }

    @ThriftConstructor
    public TaskWithConnectorType(
            int val,
            Any connectorMetadataUpdateHandleAny)
    {
        this.val = val;
        this.connectorMetadataUpdateHandleAny = connectorMetadataUpdateHandleAny;
    }

    @ThriftField(1)
    @JsonProperty
    public int getVal()
    {
        return val;
    }

    @ThriftField(2)
    public Any getConnectorMetadataUpdateHandleAny()
    {
        return connectorMetadataUpdateHandleAny;
    }

    @JsonProperty
    public ConnectorMetadataUpdateHandle getConnectorMetadataUpdateHandle()
    {
        return connectorMetadataUpdateHandle;
    }
}
