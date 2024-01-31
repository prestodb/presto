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
package com.facebook.presto.execution.executor;

import java.util.Optional;

public class QueryRecoveryDebugInfo
{
    private final QueryRecoveryState state;
    private final Optional<Long> outputBufferSize;
    private final Optional<String> outputBufferID;

    private QueryRecoveryDebugInfo(Builder builder)
    {
        this.state = builder.state;
        this.outputBufferSize = builder.outputBufferSize;
        this.outputBufferID = builder.outputBufferID;
    }

    public QueryRecoveryState getState()
    {
        return state;
    }

    public Optional<Long> getOutputBufferSize()
    {
        return outputBufferSize;
    }

    public Optional<String> getOutputBufferID()
    {
        return outputBufferID;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private QueryRecoveryState state;
        private Optional<Long> outputBufferSize = Optional.empty();
        private Optional<String> outputBufferID = Optional.empty();

        private Builder() {}

        public Builder state(QueryRecoveryState state)
        {
            this.state = state;
            return this;
        }

        public Builder outputBufferSize(Long outputBufferSize)
        {
            this.outputBufferSize = Optional.of(outputBufferSize);
            return this;
        }

        public Builder outputBufferID(String outputBufferID)
        {
            this.outputBufferID = Optional.of(outputBufferID);
            return this;
        }

        public QueryRecoveryDebugInfo build()
        {
            return new QueryRecoveryDebugInfo(this);
        }
    }
}
