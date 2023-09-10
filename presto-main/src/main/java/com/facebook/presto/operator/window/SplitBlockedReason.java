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
package com.facebook.presto.operator.window;

public enum SplitBlockedReason
{
    REVOKE,
    DELETE,
    EXCHANGE,
    HASH_BUILD,
    HASH_SEMI_JOIN,
    LOCAL_EXCHANGE_SINK,
    LOCAL_EXCHANGE_SOURCE,
    LOCAL_MERGE_SOURCE,
    LOOKUP_JOIN,
    LOOKUP_OUTER,
    MERGE,
    NESTED_LOOP_BUILD,
    NESTED_LOOP_JOIN,
    OPTIMIZED_PARTITION,
    PAGE_BUFFER,
    PAGE_SOURCE,
    PARTITIONED_OUTPUT,
    SCAN_FILTER_PROJECT,
    SPACIAL_INDEX,
    TABLE_FINISH,
    TABLE_SCAN,
    TABLE_WRITER_MERGE,
    TABLE_WRITER,
    TASK_OUTPUT,
    MEMORY,
    REVOCABLE_MEMORY,
}
