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
package com.facebook.presto.client;

import static java.util.Objects.requireNonNull;

public enum GCSOAuthScope
{
    CLOUD_PLATFORM("https://www.googleapis.com/auth/cloud-platform"),
    CLOUD_PLATFORM_READ_ONLY("https://www.googleapis.com/auth/cloud-platform.read-only"),
    DEVSTORAGE_FULL_CONTROL("https://www.googleapis.com/auth/devstorage.full_control"),
    DEVSTORAGE_READ_ONLY("https://www.googleapis.com/auth/devstorage.read_only"),
    DEVSTORAGE_READ_WRITE("https://www.googleapis.com/auth/devstorage.read_write");

    private final String scopeURL;

    GCSOAuthScope(String scopeURL)
    {
        this.scopeURL = requireNonNull(scopeURL);
    }

    public String getScopeURL()
    {
        return scopeURL;
    }
}
