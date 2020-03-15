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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

// TODO: Use builder pattern for SplitContext if we are to add optional field
public class SplitContext
{
    public static final SplitContext NON_CACHEABLE = new SplitContext(false);

    private final boolean cacheable;

    @JsonCreator
    public SplitContext(@JsonProperty boolean cacheable)
    {
        this.cacheable = cacheable;
    }

    @JsonProperty
    public boolean isCacheable()
    {
        return cacheable;
    }
}
