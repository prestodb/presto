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

import com.facebook.presto.client.QueryResults;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class LazyOutput
{
    private final boolean isFailed;
    private final boolean isRemote;
    private final Optional<QueryOutputInfo> sqlQueryOutput;
    private final Optional<QueryResults> dispatchQueryOutput;

    public LazyOutput(boolean isFailed, boolean isRemote, Optional<QueryOutputInfo> sqlQueryOutput, Optional<QueryResults> dispatchQueryOutput)
    {
        this.isFailed = isFailed;
        this.isRemote = isRemote;
        this.sqlQueryOutput = requireNonNull(sqlQueryOutput, "sqlQueryOutput is null");
        this.dispatchQueryOutput = requireNonNull(dispatchQueryOutput, "dispatchQueryOutput is null");
    }

    public boolean isFailed()
    {
        return isFailed;
    }

    public boolean isRemote()
    {
        return isRemote;
    }

    public Optional<QueryOutputInfo> getSqlQueryOutput()
    {
        return sqlQueryOutput;
    }

    public Optional<QueryResults> getDispatchQueryOutput()
    {
        return dispatchQueryOutput;
    }
}
