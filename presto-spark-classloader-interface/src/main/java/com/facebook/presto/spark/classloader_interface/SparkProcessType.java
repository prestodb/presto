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
package com.facebook.presto.spark.classloader_interface;

public enum SparkProcessType
{
    DRIVER,
    // --- backward-compatibility START
    // these 2 options are retained for backward compatibility
    // with presto-facebook repo
    // will delete them once presto-facebook change
    // to use the new values is rolled out
    EXECUTOR,
    LOCAL_EXECUTOR,
    // --- backward-compatibility END
    REMOTE_JAVA_EXECUTOR,
    LOCAL_THREAD_JAVA_EXECUTOR,
    REMOTE_CPP_EXECUTOR
}
