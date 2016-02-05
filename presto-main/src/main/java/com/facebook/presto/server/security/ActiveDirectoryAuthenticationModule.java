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
package com.facebook.presto.server.security;

import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.multibindings.Multibinder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.http.server.TheServlet;
import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.UsernamePasswordToken;
import org.apache.shiro.mgt.DefaultSecurityManager;
import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.realm.activedirectory.ActiveDirectoryRealm;
import org.apache.shiro.realm.ldap.JndiLdapContextFactory;
import org.apache.shiro.realm.ldap.LdapContextFactory;
import org.apache.shiro.realm.ldap.LdapUtils;
import org.apache.shiro.subject.Subject;

import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.SearchControls;
import javax.naming.ldap.LdapContext;
import javax.servlet.Filter;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;
import static org.apache.shiro.SecurityUtils.getSubject;
import static org.apache.shiro.SecurityUtils.setSecurityManager;

public class ActiveDirectoryAuthenticationModule
            extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(LdapServerConfig.class);
        LdapServerConfig ldapConfig = buildConfigObject(LdapServerConfig.class);

        if (ldapConfig.getAuthenticationEnabled()) {
            Multibinder.newSetBinder(binder, Filter.class, TheServlet.class)
            .addBinding()
            .to(LdapFilter.class)
            .in(Scopes.SINGLETON);

                binder.bind(Subject.class).toProvider(new Provider<Subject>() {
                   @Inject private Provider<SecurityManager> smProvider;

                   @Override
                   public Subject get()
                   {
                       SecurityManager securityManager = smProvider.get();
                       setSecurityManager(securityManager);
                       return getSubject();
                   }
                }).in(Singleton.class);

                binder.bind(SecurityManager.class).toProvider(new Provider<SecurityManager>() {
                    @Inject private Provider<ActiveDirectoryRealm> realmProvider;
                    @Inject private Provider<JndiLdapContextFactory> ldapContextFactoryProvider;

                    @Override
                    public SecurityManager get()
                    {
                        LdapContextFactory factory = ldapContextFactoryProvider.get();
                        ActiveDirectoryRealm realm = realmProvider.get();
                        realm.setLdapContextFactory(factory);

                        return new DefaultSecurityManager(realm);
                    }
                }).in(Singleton.class);

                binder.bind(ActiveDirectoryRealm.class).toProvider(new Provider<ActiveDirectoryRealm>() {
                    @Inject private LdapServerConfig ldapConfig;
                    @Inject private SearchControls searchControls;

                    @Override
                    public ActiveDirectoryRealm get()
                    {
                        String url = ldapConfig.getURL();
                        String systemUsername = ldapConfig.getSystemUser();
                        String systemPassword = ldapConfig.getSystemPassword();
                        String searchBase = ldapConfig.getSearchBase();

                        requireNonNull(url, "url is null");
                        requireNonNull(searchBase, "searchBase is null");
                        requireNonNull(systemUsername, "systemUsername is null");
                        requireNonNull(systemPassword, "systemPassword is null");
                        requireNonNull(searchControls, "search control is null");

                        ActiveDirectoryRealm realm = new ActiveDirectoryRealm() {
                            @Override
                            protected AuthenticationInfo queryForAuthenticationInfo(AuthenticationToken token, LdapContextFactory ldapSystemContextFactory) throws NamingException
                            {
                                UsernamePasswordToken upToken = (UsernamePasswordToken) token;
                                LdapContext ctxSystem = ldapSystemContextFactory.getSystemLdapContext();
                                LdapContext realUserCtx = null;
                                String userId = upToken.getUsername();
                                try {
                                    NamingEnumeration<javax.naming.directory.SearchResult> answers = ctxSystem.search(searchBase, "sAMAccountName=" + userId, searchControls);

                                    if (!answers.hasMore()) {
                                        throw new NamingException(String.format("User does not exist. Can't find user: %s.", userId));
                                    }

                                    javax.naming.directory.SearchResult result = answers.nextElement();
                                    String realuser = result.getNameInNamespace();

                                    realUserCtx = ldapSystemContextFactory.getLdapContext(realuser, String.valueOf(upToken.getPassword()));
                                }
                                finally {
                                    LdapUtils.closeContext(realUserCtx);
                                }

                                return buildAuthenticationInfo(upToken.getUsername(), upToken.getPassword());
                            }
                        };

                        realm.setUrl(url);
                        realm.setSearchBase(searchBase);
                        realm.setSystemUsername(systemUsername);
                        realm.setSystemPassword(systemPassword);

                        return realm;
                    }
                }).in(Singleton.class);

                binder.bind(JndiLdapContextFactory.class).toProvider(new Provider<JndiLdapContextFactory>() {
                    @Inject private LdapServerConfig ldapConfig;

                    @Override
                    public JndiLdapContextFactory get()
                    {
                        String url = ldapConfig.getURL();
                        String systemUsername = ldapConfig.getSystemUser();
                        String systemPassword = ldapConfig.getSystemPassword();

                        requireNonNull(url, "url is null");
                        requireNonNull(systemUsername, "systemUsername is null");
                        requireNonNull(systemPassword, "systemPassword is null");

                        JndiLdapContextFactory factory = new JndiLdapContextFactory();
                        factory.setUrl(url);
                        factory.setSystemUsername(systemUsername);
                        factory.setSystemPassword(systemPassword);
                        return factory;
                    }
                }).in(Singleton.class);
        }
    }

    @Provides
    private SearchControls getSearchControls()
    {
        SearchControls cons = new SearchControls();
        cons.setSearchScope(SearchControls.SUBTREE_SCOPE);
        String[] attrIDs = {"distinguishedName", "sn", "givenname", "mail", "telephonenumber", "memberOf", "thumbnailPhoto"};
        cons.setReturningAttributes(attrIDs);
        return cons;
    }
}
