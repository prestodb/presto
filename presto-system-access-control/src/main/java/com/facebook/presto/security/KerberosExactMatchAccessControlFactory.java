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
package com.facebook.presto.security;

import com.facebook.presto.spi.security.SystemAccessControl;
import com.facebook.presto.spi.security.SystemAccessControlFactory;

import java.util.Map;

public class KerberosExactMatchAccessControlFactory
        implements SystemAccessControlFactory
{
    @Override
    public String getName()
    {
        return "kerberos-exact-match";
    }

    @Override
    public SystemAccessControl create(Map<String, String> config)
    {
        return new KerberosExactMatchAccessControl();
    }
}
