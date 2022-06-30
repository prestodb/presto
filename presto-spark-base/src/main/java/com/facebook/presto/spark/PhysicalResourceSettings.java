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
package com.facebook.presto.spark;

public class PhysicalResourceSettings
{
    private final int executorCount;
    private final int hashPartitionCount;

    public PhysicalResourceSettings(int executorCount, int hashPartitionCount)
    {
        this.executorCount = executorCount;
        this.hashPartitionCount = hashPartitionCount;
    }

    public int getHashPartitionCount()
    {
        return hashPartitionCount;
    }

    public int getExecutorCount()
    {
        return executorCount;
    }

    public boolean isValid()
    {
        return ((executorCount > 0) && (hashPartitionCount > 0));
    }
}
