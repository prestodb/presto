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
package com.facebook.presto;

import com.facebook.presto.spi.PrestoException;
import io.airlift.units.DataSize;

import static com.facebook.presto.spi.StandardErrorCode.EXCEEDED_MEMORY_LIMIT;
import static java.lang.String.format;

public class ExceededMemoryLimitException
        extends PrestoException
{
    private final DataSize maxMemory;

    public static ExceededMemoryLimitException exceededGlobalUserLimit(DataSize maxMemory)
    {
        return new ExceededMemoryLimitException(maxMemory, format("Query exceeded max user memory size of %s", maxMemory));
    }

    public static ExceededMemoryLimitException exceededGlobalTotalLimit(DataSize maxMemory)
    {
        return new ExceededMemoryLimitException(maxMemory, format("Query exceeded max total memory size of %s", maxMemory));
    }

    public static ExceededMemoryLimitException exceededLocalUserMemoryLimit(DataSize maxMemory)
    {
        return new ExceededMemoryLimitException(maxMemory, format("Query exceeded local user memory limit of %s", maxMemory));
    }

    public static ExceededMemoryLimitException exceededLocalTotalMemoryLimit(DataSize maxMemory)
    {
        return new ExceededMemoryLimitException(maxMemory, format("Query exceeded local total memory limit of %s", maxMemory));
    }

    private ExceededMemoryLimitException(DataSize maxMemory, String message)
    {
        super(EXCEEDED_MEMORY_LIMIT, message);
        this.maxMemory = maxMemory;
    }

    public DataSize getMaxMemory()
    {
        return maxMemory;
    }
}
