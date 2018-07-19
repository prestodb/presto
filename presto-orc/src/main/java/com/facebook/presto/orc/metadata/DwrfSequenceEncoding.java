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
package com.facebook.presto.orc.metadata;

import com.facebook.presto.orc.proto.DwrfProto;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class DwrfSequenceEncoding
{
    private final DwrfProto.KeyInfo key;
    private final ColumnEncoding valueEncoding;

    public DwrfSequenceEncoding(DwrfProto.KeyInfo key, ColumnEncoding valueEncoding)
    {
        this.key = requireNonNull(key, "key is null");
        this.valueEncoding = requireNonNull(valueEncoding, "valueEncoding is null");
    }

    public DwrfProto.KeyInfo getKey()
    {
        return key;
    }

    public ColumnEncoding getValueEncoding()
    {
        return valueEncoding;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("key", key)
                .add("valueEncoding", valueEncoding)
                .toString();
    }
}
