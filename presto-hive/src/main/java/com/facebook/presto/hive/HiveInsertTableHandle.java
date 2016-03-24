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
package com.facebook.presto.hive;

import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class HiveInsertTableHandle
        extends HiveWritableTableHandle
        implements ConnectorInsertTableHandle
{
    @JsonCreator
    public HiveInsertTableHandle(
            @JsonProperty("clientId") String clientId,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("inputColumns") List<HiveColumnHandle> inputColumns,
            @JsonProperty("filePrefix") String filePrefix,
            @JsonProperty("locationHandle") LocationHandle locationHandle,
            @JsonProperty("tableStorageFormat") HiveStorageFormat tableStorageFormat,
            @JsonProperty("partitionStorageFormat") HiveStorageFormat partitionStorageFormat)
    {
        super(
                clientId,
                schemaName,
                tableName,
                inputColumns,
                filePrefix,
                locationHandle,
                tableStorageFormat,
                partitionStorageFormat);
    }
}
