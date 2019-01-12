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
package io.prestosql.operator;

/**
 * The interface is used when a function call needs to yield/pause during the process.
 * Users need to call {@link #process()} until it returns True before getting the result.
 */
public interface Work<T>
{
    /**
     * Call the method to do the work.
     * The caller can keep calling this method until it returns true (i.e., the work is done).
     *
     * @return boolean to indicate if the work has finished.
     */
    boolean process();

    /**
     * Get the result once the work is done.
     */
    T getResult();
}
