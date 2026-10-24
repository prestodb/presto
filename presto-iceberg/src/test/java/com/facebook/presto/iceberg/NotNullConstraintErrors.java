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

/**
 * Expected-error patterns for the checks that keep NOT NULL columns free of NULL values.
 */
public final class NotNullConstraintErrors
{
    private NotNullConstraintErrors() {}

    public static String nullValuesError(String column, String branch)
    {
        String remedy = branch.equals("main")
                ? "Update or delete those rows, then run rewrite_data_files with the rewrite-all option"
                : "Drop or replace the branch";
        return ".*Cannot set NOT NULL on column '" + column + "' of table '.*': data file '.*' on branch '" + branch +
                "' has NULL values in the column\\. " + remedy;
    }

    public static String noNullCountError(String column)
    {
        return ".*Cannot set NOT NULL on column '" + column + "' of table '.*': data file '.*' on branch '.*' has no NULL count for the column";
    }

    public static String moveBranchError(String column)
    {
        return ".*Cannot move branch '.*' of table '.*' to snapshot [0-9]+: data file '.*' of that snapshot can hold NULL values in NOT NULL column '" +
                column + "'\\. Drop the NOT NULL constraint first";
    }
}
