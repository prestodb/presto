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
package com.facebook.presto.spi.memory;

public interface CacheEntry
{
    // Returns the cached data.
    byte[] getData();

    // Returns true if the data is fetched.
    boolean isFetched();

    // Returns true if the calling thread is expected to fetch the
    // content. If so, after fetching, the calling thread must call
    // setFetched().
    boolean needFetch();

    // Waits until another thread has fetched the data. This should be
    // called if both isFetched() and needFetch() are false.
    void waitFetched();

    // Sets the state to fetched. This may only be called if the
    // calling tread has previously received true from needFetch().
    void setFetched();

    // Indicates that the calling thread is finished with this. The
    // calling thread must not keep references to either this or the
    // value returned by getData().
    void release();
}
