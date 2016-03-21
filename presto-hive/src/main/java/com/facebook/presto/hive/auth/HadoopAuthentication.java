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

package com.facebook.presto.hive.auth;

import org.apache.hadoop.security.UserGroupInformation;

import java.security.PrivilegedAction;

public interface HadoopAuthentication
{
    void authenticate();

    UserGroupInformation getUserGroupInformation();

    default <T> T doAs(PrivilegedAction<T> action)
    {
        return getUserGroupInformation()
                .doAs(action);
    }

    default void doAs(Runnable action)
    {
        getUserGroupInformation()
                .doAs((PrivilegedAction<Object>) () -> {
                    action.run();
                    return null;
                });
    }

    default <T> T doAs(String user, PrivilegedAction<T> action)
    {
        return UserGroupInformation
                .createProxyUser(user, getUserGroupInformation())
                .doAs(action);
    }

    default void doAs(String user, Runnable action)
    {
        UserGroupInformation
                .createProxyUser(user, getUserGroupInformation())
                .doAs((PrivilegedAction<Object>) () -> {
                    action.run();
                    return null;
                });
    }
}
