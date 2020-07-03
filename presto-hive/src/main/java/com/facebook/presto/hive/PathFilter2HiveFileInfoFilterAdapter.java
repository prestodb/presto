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

import org.apache.hadoop.fs.PathFilter;

import static java.util.Objects.requireNonNull;

public class PathFilter2HiveFileInfoFilterAdapter
        implements HiveFileInfoFilter
{
    private final PathFilter pathFilter;

    public PathFilter2HiveFileInfoFilterAdapter(PathFilter pathFilter)
    {
        this.pathFilter = requireNonNull(pathFilter, "pathFilter is null");
    }

    @Override
    public boolean accept(HiveFileInfo file)
    {
        return pathFilter.accept(file.getPath());
    }
}
