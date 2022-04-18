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
package com.facebook.presto.resourcemanager.cpu;

import com.sun.management.OperatingSystemMXBean;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

import java.lang.management.ManagementFactory;

import static com.facebook.presto.PrestoMediaTypes.APPLICATION_JACKSON_SMILE;
import static com.facebook.presto.server.security.RoleType.INTERNAL;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

@Path("/v1/cpu")
@RolesAllowed(INTERNAL)
public class CPUResource
{
    private OperatingSystemMXBean operatingSystemMXBean;

    @Inject
    public CPUResource()
    {
        if (ManagementFactory.getOperatingSystemMXBean() instanceof OperatingSystemMXBean) {
            // we want the com.sun.management sub-interface of java.lang.management.OperatingSystemMXBean
            this.operatingSystemMXBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        }
    }

    @POST
    @Produces({APPLICATION_JSON, APPLICATION_JACKSON_SMILE})
    @Consumes({APPLICATION_JSON, APPLICATION_JACKSON_SMILE})
    public CPUInfo getCPUInfo()
    {
        return new CPUInfo(operatingSystemMXBean.getProcessCpuLoad() * 100);
    }
}
