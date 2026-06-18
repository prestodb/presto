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

package com.facebook.presto.hudi;

public enum HudiTableType
{
    COW,
    MOR,
    UNKNOWN,
    /**/;

    public static HudiTableType fromInputFormat(String inputFormat)
    {
        switch (inputFormat) {
            case "org.apache.hudi.hadoop.HoodieParquetInputFormat":
            case "com.uber.hoodie.hadoop.HoodieInputFormat":
                return COW;
            case "org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat":
            case "com.uber.hoodie.hadoop.realtime.HoodieRealtimeInputFormat":
                return MOR;
            default:
                return UNKNOWN;
        }
    }
}
