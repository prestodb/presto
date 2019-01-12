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
package io.prestosql.operator.exchange;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.operator.OperatorInfo;
import io.prestosql.util.Mergeable;

public class LocalExchangeBufferInfo
        implements Mergeable<LocalExchangeBufferInfo>, OperatorInfo
{
    private final long bufferedBytes;
    private final int bufferedPages;

    @JsonCreator
    public LocalExchangeBufferInfo(
            @JsonProperty("bufferedBytes") long bufferedBytes,
            @JsonProperty("bufferedPages") int bufferedPages)
    {
        this.bufferedBytes = bufferedBytes;
        this.bufferedPages = bufferedPages;
    }

    @JsonProperty
    public long getBufferedBytes()
    {
        return bufferedBytes;
    }

    @JsonProperty
    public int getBufferedPages()
    {
        return bufferedPages;
    }

    @Override
    public LocalExchangeBufferInfo mergeWith(LocalExchangeBufferInfo other)
    {
        return new LocalExchangeBufferInfo(bufferedBytes + other.getBufferedBytes(), bufferedPages + other.getBufferedPages());
    }
}
