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
package com.facebook.presto.execution;

import com.facebook.presto.sql.tree.Statement;

import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.Executor;

/**
 * Classes implementing this interface must be thread safe. That is, all the methods listed below
 * may be called concurrently from any thread.
 */
@ThreadSafe
public interface QueryQueueManager
{
    boolean submit(Statement statement, QueryExecution queryExecution, SqlQueryManagerStats stats);
    void setExecutor(Executor executor);
}
