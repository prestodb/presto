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

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import com.facebook.airlift.log.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class MetastoreCustomAuthConfig
{
    private String lhContext;
    private static final Logger log = Logger.get(MetastoreCustomAuthConfig.class);
    private String hiveMetastoreClientPlainUsername;
    private String hiveMetastoreClientPlainToken;
    public static final String LH_CONTEXT = "LH_CONTEXT";
    public static final String LH_INSTANCE_NAME = "LH_INSTANCE_NAME";
    public static final String LH_INSTANCE_SECRET = "LH_INSTANCE_SECRET";
    public static final String SAAS_NAME = "/secrets/internal-service-auth/internal_svc_auth_user";
    public static final String SAAS_SECRET = "/secrets/internal-service-auth/internal_svc_auth_password";

    public MetastoreCustomAuthConfig()
    {
        this.lhContext = System.getenv(LH_CONTEXT);
    }

    @Config("hive.metastore.client.plain.username")
    @ConfigDescription("Hive Metastore client plain username")
    public MetastoreCustomAuthConfig setHiveMetastoreClientPlainUsername(String hiveMetastoreClientPlainUsername)
    {
        this.hiveMetastoreClientPlainUsername = hiveMetastoreClientPlainUsername;
        return this;
    }

    public String getHiveMetastoreClientPlainUsername()
    {
        String userName;
        try {
            if (lhContext.equals("sw_dev") || lhContext.equals("sw_ent") || lhContext.equals("sw_env")) {
                userName = System.getenv(LH_INSTANCE_NAME);
                log.info("dev or standalone - username : " + userName);
            }
            else {
                userName = saasUserName();
            }
        }
        catch (Exception exp) {
            userName = saasUserName();
        }
        return userName;
    }

    @Config("hive.metastore.client.plain.token")
    @ConfigDescription("Hive Metastore client plain token")
    public MetastoreCustomAuthConfig setHiveMetastoreClientPlainToken(String hiveMetastoreClientPlainToken)
    {
        this.hiveMetastoreClientPlainToken = hiveMetastoreClientPlainToken;
        return this;
    }

    public String getHiveMetastoreClientPlainToken()
    {
        String token;
        String tToken;
        String name;
        LocalDate dateObj = LocalDate.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy");
        String date = dateObj.format(formatter);
        try {
            if (lhContext.equals("sw_dev") || lhContext.equals("sw_ent") || lhContext.equals("sw_env")) {
                tToken = System.getenv(LH_INSTANCE_SECRET);
                name = System.getenv(LH_INSTANCE_NAME);
                token = (tToken != null) ? tToken : (name + "-" + date);
            }
            else {
                token = saasPassword(date);
            }
        }
        catch (Exception exp) {
            token = saasPassword(date);
        }
        return token;
    }

    private String saasUserName()
    {
        String userName;
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(SAAS_NAME));
            String name = bufferedReader.readLine();
            bufferedReader.close();
            userName = name;
            log.info("Saas - username : " + userName);
        }
        catch (Exception e) {
            userName = this.hiveMetastoreClientPlainUsername;
            log.info("Reached exception - username : " + userName);
        }
        return userName;
    }

    private String saasPassword(String date)
    {
        String tToken;
        String token;
        String name;
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(SAAS_SECRET));
            tToken = bufferedReader.readLine();
            bufferedReader.close();
            token = tToken;
        }
        catch (Exception e) {
            try {
                BufferedReader bufferedReader = new BufferedReader(new FileReader(SAAS_NAME));
                name = bufferedReader.readLine();
                bufferedReader.close();
                token = (name + "-" + date);
            }
            catch (Exception exp) {
                token = this.hiveMetastoreClientPlainToken;
            }
        }
        return token;
    }
}
