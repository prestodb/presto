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
package com.facebook.presto.sql.planner;

/**
 * Receives feedback from the execution stage. A worker node can send output
 * back to the coordinator that is associated with a plan node.
 */
public interface OutputReceiver
{
    /**
     * Called with an object returned by a worker. An object is received at least once and can be
     * received multiple times.
     */
    void updateOutput(Object value);
}
