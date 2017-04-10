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
package com.facebook.presto.spi;

import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;

import java.util.Collection;
import java.util.List;

public interface RecordSink
{
    void beginRecord();

    void finishRecord();

    void appendNull();

    void appendBoolean(boolean value);

    void appendLong(long value);

    void appendDouble(double value);

    void appendString(byte[] value);

    void appendObject(Object value);

    Collection<Slice> commit();

    void rollback();

    List<Type> getColumnTypes();
}
