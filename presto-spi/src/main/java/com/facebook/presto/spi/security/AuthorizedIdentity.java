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
package com.facebook.presto.spi.security;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class AuthorizedIdentity
{
    private final String userName;
    private final Optional<String> reasonForSelect;
    private final Optional<Boolean> delegationCheckResult;

    public AuthorizedIdentity(String userName, String reasonForSelect, Boolean delegationCheckResult)
    {
        this.userName = requireNonNull(userName, "userName is null");
        this.reasonForSelect = Optional.ofNullable(reasonForSelect);
        this.delegationCheckResult = Optional.ofNullable(delegationCheckResult);
    }

    public String getUserName()
    {
        return userName;
    }

    public Optional<String> getReasonForSelect()
    {
        return reasonForSelect;
    }

    public Optional<Boolean> getDelegationCheckResult()
    {
        return delegationCheckResult;
    }
}
