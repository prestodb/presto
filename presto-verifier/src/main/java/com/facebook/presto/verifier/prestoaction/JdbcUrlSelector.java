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
package com.facebook.presto.verifier.prestoaction;

import com.google.common.collect.Iterators;

import javax.annotation.concurrent.ThreadSafe;

import java.util.Iterator;
import java.util.List;

@ThreadSafe
public class JdbcUrlSelector
        implements Iterator<String>
{
    private final Iterator<String> jdbcUrls;

    public JdbcUrlSelector(List<String> jdbcUrls)
    {
        this.jdbcUrls = Iterators.cycle(jdbcUrls);
    }

    @Override
    public boolean hasNext()
    {
        return jdbcUrls.hasNext();
    }

    @Override
    public synchronized String next()
    {
        return jdbcUrls.next();
    }
}
