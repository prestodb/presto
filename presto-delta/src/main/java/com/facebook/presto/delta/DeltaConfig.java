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
package com.facebook.presto.delta;

import com.facebook.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

public class DeltaConfig
{
    private int maxSplitsBatchSize = 200;
    private boolean parquetDereferencePushdownEnabled = true;

    @NotNull
    public boolean isParquetDereferencePushdownEnabled()
    {
        return parquetDereferencePushdownEnabled;
    }

    @Config("delta.parquet-dereference-pushdown-enabled")
    public DeltaConfig setParquetDereferencePushdownEnabled(boolean parquetDereferencePushdownEnabled)
    {
        this.parquetDereferencePushdownEnabled = parquetDereferencePushdownEnabled;
        return this;
    }

    public int getMaxSplitsBatchSize()
    {
        return maxSplitsBatchSize;
    }

    @Config("delta.max-splits-batch-size")
    public DeltaConfig setMaxSplitsBatchSize(int maxSplitsBatchSize)
    {
        this.maxSplitsBatchSize = maxSplitsBatchSize;
        return this;
    }
}
