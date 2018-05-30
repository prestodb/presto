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
package com.facebook.presto.hive.authentication;

public interface HdfsAuthentication
{
    <R, E extends Exception> R doAs(String user, GenericExceptionAction<R, E> action)
            throws E;

    default void doAs(String user, Runnable action)
    {
        doAs(user, () -> {
            action.run();
            return null;
        });
    }
}
