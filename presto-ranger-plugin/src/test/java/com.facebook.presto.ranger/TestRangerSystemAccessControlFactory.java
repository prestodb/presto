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
package com.facebook.presto.ranger;

import org.testng.annotations.Test;

import java.util.HashMap;

public class TestRangerSystemAccessControlFactory
{
    @Test
    public void testGetName() throws Exception
    {
    }

    @Test
    public void testCreate() throws Exception
    {
        RangerSystemAccessControlFactory rangerSystemAccessControlFactory = new RangerSystemAccessControlFactory();
        rangerSystemAccessControlFactory.create(new HashMap<String, String>());

        rangerSystemAccessControlFactory.create(new HashMap<String, String>()
        {
            {
                put("login-to-ranger-principal", "");
                put("login-to-ranger-keytab", "");
            }
        });
    }
}
