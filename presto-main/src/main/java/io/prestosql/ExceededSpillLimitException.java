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
package io.prestosql;

import io.airlift.units.DataSize;
import io.prestosql.spi.PrestoException;

import static io.prestosql.spi.StandardErrorCode.EXCEEDED_SPILL_LIMIT;
import static java.lang.String.format;

public class ExceededSpillLimitException
        extends PrestoException
{
    public static ExceededSpillLimitException exceededLocalLimit(DataSize maxSpill)
    {
        return new ExceededSpillLimitException(format("Query exceeded local spill limit of %s", maxSpill));
    }

    public static ExceededSpillLimitException exceededPerQueryLocalLimit(DataSize maxSpill)
    {
        return new ExceededSpillLimitException(format("Query exceeded per-query local spill limit of %s", maxSpill));
    }

    private ExceededSpillLimitException(String message)
    {
        super(EXCEEDED_SPILL_LIMIT, message);
    }
}
