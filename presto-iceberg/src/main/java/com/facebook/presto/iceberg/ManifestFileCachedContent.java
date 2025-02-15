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
package com.facebook.presto.iceberg;

import java.nio.ByteBuffer;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class ManifestFileCachedContent
{
    private final List<ByteBuffer> data;
    private final long length;

    public ManifestFileCachedContent(final List<ByteBuffer> data, long length)
    {
        this.data = requireNonNull(data, "data is null");
        this.length = length;
    }

    public List<ByteBuffer> getData()
    {
        return data;
    }

    public long getLength()
    {
        return length;
    }
}
