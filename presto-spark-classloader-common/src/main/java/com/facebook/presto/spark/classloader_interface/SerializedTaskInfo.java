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

import java.io.Serializable;

import static java.util.Objects.requireNonNull;

public class SerializedTaskInfo
        implements Serializable
{
    private byte[] bytes;

    public SerializedTaskInfo(byte[] bytes)
    {
        this.bytes = requireNonNull(bytes, "bytes is null");
    }

    public byte[] getBytes()
    {
        return requireNonNull(bytes, "bytes is null");
    }

    public byte[] getBytesAndClear()
    {
        byte[] result = getBytes();
        bytes = null;
        return result;
    }

    @Override
    // Displayed at the Spark web interface
    public String toString()
    {
        return "";
    }
}
