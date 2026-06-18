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
package com.facebook.presto.plugin.bigquery;

import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

public class MockResponsesBatch
        implements Iterator<ReadRowsResponse>
{
    private Queue<Object> responses = new LinkedList<>();

    void addResponse(ReadRowsResponse response)
    {
        responses.add(response);
    }

    void addException(RuntimeException exception)
    {
        responses.add(exception);
    }

    @Override
    public boolean hasNext()
    {
        return !responses.isEmpty();
    }

    @Override
    public ReadRowsResponse next()
    {
        Object next = responses.poll();
        if (next instanceof ReadRowsResponse) {
            return (ReadRowsResponse) next;
        }
        if (next instanceof RuntimeException) {
            throw (RuntimeException) next;
        }
        return null;
    }
}
