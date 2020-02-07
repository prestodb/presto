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
package com.facebook.presto.druid.zip;

import java.util.LinkedHashMap;
import java.util.Map;

class ExtraDataList
{
    private final Map<Short, ExtraData> entries;

    public ExtraDataList()
    {
        entries = new LinkedHashMap<>();
    }

    public ExtraDataList(ExtraDataList other)
    {
        entries = new LinkedHashMap<>(other.getEntries());
    }

    public Map<Short, ExtraData> getEntries()
    {
        return entries;
    }

    public ExtraDataList(ExtraData... extra)
    {
        this();
        for (ExtraData e : extra) {
            add(e);
        }
    }

    public void add(ExtraData entry)
    {
        if (getLength() + entry.getLength() > 0xffff) {
            throw new IllegalArgumentException("adding entry will make the extra field be too long");
        }
        entries.put(entry.getId(), entry);
    }

    public ExtraDataList(byte[] buffer)
    {
        if (buffer.length > 0xffff) {
            throw new IllegalArgumentException("invalid extra field length");
        }
        entries = new LinkedHashMap<>();
        int index = 0;
        while (index < buffer.length) {
            ExtraData extra = new ExtraData(buffer, index);
            entries.put(extra.getId(), extra);
            index += extra.getLength();
        }
    }

    public ExtraData get(short id)
    {
        return entries.get(id);
    }

    public int getLength()
    {
        int length = 0;
        for (ExtraData e : entries.values()) {
            length += e.getLength();
        }
        return length;
    }
}
