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

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.metadata.ColumnPath;

/**
 * This is a test implementation of the AnonymizationManager interface.
 * It will anonymize any column named "last_name" by taking the first letter of the value
 * and replacing the rest with stars.
 * It is used for testing purposes only.
 */
public class TestAnonymizationManager
        implements AnonymizationManager
{
    public static AnonymizationManager create(Configuration conf, String tableName)
    {
        return new TestAnonymizationManager();
    }

    @Override
    public boolean shouldAnonymize(ColumnPath columnPath)
    {
        // Matches last_name
        return columnPath.toDotString().equals("last_name");
    }

    @Override
    public String anonymize(ColumnPath columnPath, String value)
    {
        // This is a trivial anonymization method for testing purposes
        if (shouldAnonymize(columnPath)) {
            if (value == null || value.isEmpty()) {
                return value;
            }
            // Take first letter of value and star the rest
            return value.charAt(0) + "****";
        }
        return value;
    }
}
