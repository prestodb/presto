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
package com.facebook.presto.hive;

import com.facebook.airlift.log.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class CustomMetastoreAuthConfigUtils
{
    private static final Logger log = Logger.get(CustomMetastoreAuthConfigUtils.class);
    private static final String LH_CONTEXT = "LH_CONTEXT";
    private static final String LH_INSTANCE_NAME = "LH_INSTANCE_NAME";
    private static final String LH_INSTANCE_SECRET = "LH_INSTANCE_SECRET";
    private static final String SAAS_NAME = "/secrets/internal-service-auth/internal_svc_auth_user";
    private static final String SAAS_SECRET = "/secrets/internal-service-auth/internal_svc_auth_password";
    private static final String lhContext = System.getenv(LH_CONTEXT);

    private CustomMetastoreAuthConfigUtils()
    {}

    public static String getMetastoreUsername()
    {
        String userName;
        try {
            if ("sw_dev".equals(lhContext) || "sw_ent".equals(lhContext) || "sw_env".equals(lhContext)) {
                userName = System.getenv(LH_INSTANCE_NAME);
                log.info("dev or standalone - username : " + userName);
            }
            else {
                userName = getSaasUserName();
            }
        }
        catch (Exception exp) {
            userName = getSaasUserName();
        }
        return userName;
    }

    public static String getMetastoreToken()
    {
        String token;
        String tToken;
        String name;
        LocalDate dateObj = LocalDate.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy");
        String date = dateObj.format(formatter);
        try {
            if ("sw_dev".equals(lhContext) || "sw_ent".equals(lhContext) || "sw_env".equals(lhContext)) {
                tToken = System.getenv(LH_INSTANCE_SECRET);
                name = System.getenv(LH_INSTANCE_NAME);
                token = (tToken != null) ? tToken : (name + "-" + date);
            }
            else {
                token = getSaasPassword(date);
            }
        }
        catch (Exception exp) {
            token = getSaasPassword(date);
        }
        return token;
    }

    private static String getSaasUserName()
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
            userName = null;
            log.info("Reached exception - username : " + userName);
        }
        return userName;
    }

    private static String getSaasPassword(String date)
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
                token = null;
            }
        }
        return token;
    }
}
