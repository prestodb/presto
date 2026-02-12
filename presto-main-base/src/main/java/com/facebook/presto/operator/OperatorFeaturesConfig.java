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
package com.facebook.presto.operator;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import jakarta.validation.constraints.NotNull;

public class OperatorFeaturesConfig
{
    private int tableFinishInfoJsonLengthLimit = 10_000_000;

    @NotNull
    public int getTableFinishInfoJsonLengthLimit()
    {
        return tableFinishInfoJsonLengthLimit;
    }

    @Config("table-finish-info-json-length-limit")
    @ConfigDescription("Maximum number of characters in connector output metadata JSON in table finish info")
    public OperatorFeaturesConfig setTableFinishInfoJsonLengthLimit(int tableFinishInfoJsonLengthLimit)
    {
        this.tableFinishInfoJsonLengthLimit = tableFinishInfoJsonLengthLimit;
        return this;
    }
}
