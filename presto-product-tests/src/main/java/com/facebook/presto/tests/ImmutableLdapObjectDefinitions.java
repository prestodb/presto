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
package com.facebook.presto.tests;

import com.google.common.collect.ImmutableMap;
import io.prestodb.tempto.fulfillment.ldap.LdapObjectDefinition;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.lang.String.format;

public final class ImmutableLdapObjectDefinitions
{
    private static final String DOMAIN = "dc=presto,dc=testldap,dc=com";
    private static final String AMERICA_DISTINGUISHED_NAME = format("ou=America,%s", DOMAIN);
    private static final String ASIA_DISTINGUISHED_NAME = format("ou=Asia,%s", DOMAIN);
    private static final String LDAP_PASSWORD = "LDAPPass123";
    private static final String MEMBER_OF = "memberOf";
    private static final String MEMBER = "member";

    private ImmutableLdapObjectDefinitions()
    {}

    public static final LdapObjectDefinition AMERICA_ORG = buildLdapOrganizationObject("America", AMERICA_DISTINGUISHED_NAME, "America");

    public static final LdapObjectDefinition ASIA_ORG = buildLdapOrganizationObject("Asia", ASIA_DISTINGUISHED_NAME, "Asia");

    public static final LdapObjectDefinition DEFAULT_GROUP = buildLdapGroupObject("DefaultGroup", "DefaultGroupUser", Optional.of(Arrays.asList("ChildGroup")));

    public static final LdapObjectDefinition PARENT_GROUP = buildLdapGroupObject("ParentGroup", "ParentGroupUser", Optional.of(Arrays.asList("DefaultGroup")));

    public static final LdapObjectDefinition CHILD_GROUP = buildLdapGroupObject("ChildGroup", "ChildGroupUser", Optional.empty());

    public static final LdapObjectDefinition DEFAULT_GROUP_USER = buildLdapUserObject("DefaultGroupUser", Optional.of(Arrays.asList("DefaultGroup")), LDAP_PASSWORD);

    public static final LdapObjectDefinition PARENT_GROUP_USER = buildLdapUserObject("ParentGroupUser", Optional.of(Arrays.asList("ParentGroup")), LDAP_PASSWORD);

    public static final LdapObjectDefinition CHILD_GROUP_USER = buildLdapUserObject("ChildGroupUser", Optional.of(Arrays.asList("ChildGroup")), LDAP_PASSWORD);

    public static final LdapObjectDefinition ORPHAN_USER = buildLdapUserObject("OrphanUser", Optional.empty(), LDAP_PASSWORD);

    public static final LdapObjectDefinition SPECIAL_USER = buildLdapUserObject("User WithSpecialPwd", Optional.of(Arrays.asList("DefaultGroup")), "LDAP:Pass ~!@#$%^&*()_+{}|:\"<>?/.,';\\][=-`");

    public static final LdapObjectDefinition USER_IN_MULTIPLE_GROUPS = buildLdapUserObject("UserInMultipleGroups", Optional.of(Arrays.asList("DefaultGroup", "ParentGroup")), LDAP_PASSWORD);

    private static LdapObjectDefinition buildLdapOrganizationObject(String id, String distinguishedName, String unit)
    {
        return LdapObjectDefinition.builder(id)
                .setDistinguishedName(distinguishedName)
                .setAttributes(ImmutableMap.of("ou", unit))
                .setObjectClasses(Arrays.asList("top", "organizationalUnit"))
                .build();
    }

    private static LdapObjectDefinition buildLdapGroupObject(String groupName, String userName, Optional<List<String>> childGroupNames)
    {
        if (childGroupNames.isPresent()) {
            return buildLdapGroupObject(groupName, AMERICA_DISTINGUISHED_NAME, userName, ASIA_DISTINGUISHED_NAME, childGroupNames, Optional.of(AMERICA_DISTINGUISHED_NAME));
        }
        else {
            return buildLdapGroupObject(groupName, AMERICA_DISTINGUISHED_NAME, userName, ASIA_DISTINGUISHED_NAME,
                    Optional.empty(), Optional.empty());
        }
    }

    private static LdapObjectDefinition buildLdapGroupObject(String groupName, String groupOrganizationName,
            String userName, String userOrganizationName, Optional<List<String>> childGroupNames, Optional<String> childGroupOrganizationName)
    {
        if (childGroupNames.isPresent() && childGroupOrganizationName.isPresent()) {
            return LdapObjectDefinition.builder(groupName)
                    .setDistinguishedName(format("cn=%s,%s", groupName, groupOrganizationName))
                    .setAttributes(ImmutableMap.of(
                            "cn", groupName,
                            "member", format("uid=%s,%s", userName, userOrganizationName)))
                    .setModificationAttributes(getAttributes(childGroupNames.get(), childGroupOrganizationName.get(), MEMBER))
                    .setObjectClasses(Arrays.asList("groupOfNames"))
                    .build();
        }
        else {
            return LdapObjectDefinition.builder(groupName)
                    .setDistinguishedName(format("cn=%s,%s", groupName, groupOrganizationName))
                    .setAttributes(ImmutableMap.of(
                            "cn", groupName,
                            "member", format("uid=%s,%s", userName, userOrganizationName)))
                    .setObjectClasses(Arrays.asList("groupOfNames"))
                    .build();
        }
    }

    private static LdapObjectDefinition buildLdapUserObject(String userName, Optional<List<String>> groupNames, String password)
    {
        if (groupNames.isPresent()) {
            return buildLdapUserObject(userName, ASIA_DISTINGUISHED_NAME,
                    groupNames, Optional.of(AMERICA_DISTINGUISHED_NAME), password);
        }
        else {
            return buildLdapUserObject(userName, ASIA_DISTINGUISHED_NAME,
                    Optional.empty(), Optional.empty(), password);
        }
    }

    private static LdapObjectDefinition buildLdapUserObject(String userName, String userOrganizationName,
            Optional<List<String>> groupNames, Optional<String> groupOrganizationName, String password)
    {
        if (groupNames.isPresent() && groupOrganizationName.isPresent()) {
            return LdapObjectDefinition.builder(userName)
                    .setDistinguishedName(format("uid=%s,%s", userName, userOrganizationName))
                    .setAttributes(ImmutableMap.of(
                            "cn", userName,
                            "sn", userName,
                            "userPassword", password))
                    .setObjectClasses(Arrays.asList("person", "inetOrgPerson"))
                    .setModificationAttributes(getAttributes(groupNames.get(), groupOrganizationName.get(), MEMBER_OF))
                    .build();
        }
        else {
            return LdapObjectDefinition.builder(userName)
                    .setDistinguishedName(format("uid=%s,%s", userName, userOrganizationName))
                    .setAttributes(ImmutableMap.of(
                            "cn", userName,
                            "sn", userName,
                            "userPassword", password))
                    .setObjectClasses(Arrays.asList("person", "inetOrgPerson"))
                    .build();
        }
    }

    private static ImmutableMap<String, List<String>> getAttributes(List<String> groupNames, String groupOrganizationName, String relation)
    {
        return ImmutableMap.of(relation, groupNames.stream()
                .map(groupName -> format("cn=%s,%s", groupName, groupOrganizationName))
                .collect(Collectors.toList()));
    }
}
