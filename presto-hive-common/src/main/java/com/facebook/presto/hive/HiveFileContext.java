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

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class HiveFileContext
{
    public static final HiveFileContext DEFAULT_HIVE_FILE_CONTEXT = new HiveFileContext(false, Optional.empty());

    private final boolean cacheable;
    private final Optional<ExtraHiveFileInfo<?>> extraFileInfo;

    public HiveFileContext(boolean cacheable, Optional<ExtraHiveFileInfo<?>> extraFileInfo)
    {
        this.cacheable = cacheable;
        this.extraFileInfo = requireNonNull(extraFileInfo, "extraFileInfo is null");
    }

    /**
     * Decide whether the current file is eligible for local cache
     */
    public boolean isCacheable()
    {
        return cacheable;
    }

    /**
     * For file opener
     */
    public Optional<ExtraHiveFileInfo<?>> getExtraFileInfo()
    {
        return extraFileInfo;
    }

    public interface ExtraHiveFileInfo<T>
    {
        T getExtraFileInfo();
    }
}
