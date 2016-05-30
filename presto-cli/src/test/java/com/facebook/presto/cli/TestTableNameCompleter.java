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
package com.facebook.presto.cli;

import com.facebook.presto.client.ClientSession;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import org.testng.annotations.Test;

import java.util.Optional;

import static org.testng.Assert.assertEquals;

public class TestTableNameCompleter
{
    @Test
    public void testAutoCompleteWithoutSchema()
    {
        ClientSession session = new ClientOptions().toClientSession();
        QueryRunner runner = QueryRunner.create(session,
                Optional.<HostAndPort>empty(),
                Optional.<String>empty(),
                Optional.<String>empty(),
                Optional.<String>empty(),
                Optional.<String>empty(),
                Optional.<String>empty(),
                Optional.<String>empty(),
                false,
                null);
        TableNameCompleter completer = new TableNameCompleter(runner);
        assertEquals(completer.complete("SELECT is_infi", 14, ImmutableList.of()), 7);
    }
}
