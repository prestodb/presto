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

package com.facebook.presto.hive.security.ranger;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

@Immutable
public class VXUser
{
    private final String createDate;
    private final String description;
    private final String firstName;
    private final List<Object> groupIdList;
    private final List<String> groupNameList;
    private final Long id;
    private final Long isVisible;
    private final String lastName;
    private final String name;
    private final String password;
    private final Long status;
    private final String updateDate;
    private final List<String> userRoleList;
    private final Long userSource;

    @JsonCreator
    public VXUser(
            @JsonProperty("createDate") String createDate,
            @JsonProperty("description") String description,
            @JsonProperty("firstName") String firstName,
            @JsonProperty("groupIdList") List<Object> groupIdList,
            @JsonProperty("groupNameList") List<String> groupNameList,
            @JsonProperty("id") Long id,
            @JsonProperty("isVisible") Long isVisible,
            @JsonProperty("lastName") String lastName,
            @JsonProperty("name") String name,
            @JsonProperty("password") String password,
            @JsonProperty("status") Long status,
            @JsonProperty("updateDate") String updateDate,
            @JsonProperty("userRoleList") List<String> userRoleList,
            @JsonProperty("userSource") Long userSource)
    {
        this.createDate = createDate;
        this.description = description;
        this.firstName = firstName;
        this.groupIdList = groupIdList;
        this.groupNameList = groupNameList;
        this.id = id;
        this.isVisible = isVisible;
        this.lastName = lastName;
        this.name = requireNonNull(name, "name is null");
        this.password = password;
        this.status = status;
        this.updateDate = updateDate;
        this.userRoleList = userRoleList == null ? null : ImmutableList.copyOf(userRoleList);
        this.userSource = userSource;
    }

    @JsonProperty
    public String getCreateDate()
    {
        return createDate;
    }

    @JsonProperty
    public String getDescription()
    {
        return description;
    }

    @JsonProperty
    public String getFirstName()
    {
        return firstName;
    }

    @JsonProperty
    public List<Object> getGroupIdList()
    {
        return groupIdList;
    }

    @JsonProperty
    public List<String> getGroupNameList()
    {
        return groupNameList;
    }

    @JsonProperty
    public Long getId()
    {
        return id;
    }

    @JsonProperty
    public Long getIsVisible()
    {
        return isVisible;
    }

    @JsonProperty
    public String getLastName()
    {
        return lastName;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public String getPassword()
    {
        return password;
    }

    @JsonProperty
    public Long getStatus()
    {
        return status;
    }

    @JsonProperty
    public String getUpdateDate()
    {
        return updateDate;
    }

    @JsonProperty
    public List<String> getUserRoleList()
    {
        return userRoleList;
    }

    @JsonProperty
    public Long getUserSource()
    {
        return userSource;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("createDate", createDate)
                .add("description", description)
                .add("firstName", firstName)
                .add("groupIdList", groupIdList)
                .add("groupNameList", groupNameList)
                .add("id", id)
                .add("isVisible", isVisible)
                .add("lastName", lastName)
                .add("password", password)
                .add("status", status)
                .add("updateDate", updateDate)
                .add("userRoleList", userRoleList)
                .add("userSource", userSource)
                .toString();
    }
}
