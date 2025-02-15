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
package org.apache.parquet.anonymization;

import org.apache.parquet.hadoop.metadata.ColumnPath;

public interface AnonymizationManager
{
    String UNSUPPORTED_ERR_MSG = "Anonymization is currently only supported on String types";

    boolean shouldAnonymize(ColumnPath columnPath);

    String anonymize(ColumnPath columnPath, String value);

    default float anonymize(ColumnPath columnPath, float value)
    {
        throw new UnsupportedOperationException(UNSUPPORTED_ERR_MSG);
    }

    default int anonymize(ColumnPath columnPath, int value)
    {
        throw new UnsupportedOperationException(UNSUPPORTED_ERR_MSG);
    }

    default double anonymize(ColumnPath columnPath, double value)
    {
        throw new UnsupportedOperationException(UNSUPPORTED_ERR_MSG);
    }

    default long anonymize(ColumnPath columnPath, long value)
    {
        throw new UnsupportedOperationException(UNSUPPORTED_ERR_MSG);
    }

    default boolean anonymize(ColumnPath columnPath, boolean value)
    {
        throw new UnsupportedOperationException(UNSUPPORTED_ERR_MSG);
    }
}
