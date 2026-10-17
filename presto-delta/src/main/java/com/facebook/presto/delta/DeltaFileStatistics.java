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
package com.facebook.presto.delta;

import java.util.Optional;

/**
 * Statistics for a single Delta Lake data file, parsed from the {@code add.stats} JSON field
 * in the Delta transaction log.
 */
public interface DeltaFileStatistics
{
    Optional<Long> getNumRecords();

    Optional<Long> getNullCount(String physicalColumnName);

    Optional<Object> getMinColumnValue(DeltaColumnHandle columnHandle);

    Optional<Object> getMaxColumnValue(DeltaColumnHandle columnHandle);
}
