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
package com.facebook.presto.cache;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class DataCacheKey
{
    private final FileToken fileToken;
    private final List<FileRange> rangeList;

    public DataCacheKey(FileToken token, List<FileRange> ranges)
    {
        this.fileToken = requireNonNull(token, "token is null");
        requireNonNull(ranges, "ranges is null");
        checkArgument(ranges.size() > 0);
        this.rangeList = ranges;
    }

    public FileToken getToken()
    {
        return fileToken;
    }

    public List<FileRange> getOffset()
    {
        return rangeList;
    }
}
