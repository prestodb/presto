/*
 * Copyright 2013-2016, Teradata, Inc. All rights reserved.
 */
package com.teradata.benchto.service.rest;

import com.teradata.benchto.service.EnvironmentService;
import com.teradata.benchto.service.model.Environment;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

import static org.springframework.web.bind.annotation.RequestMethod.GET;
import static org.springframework.web.bind.annotation.RequestMethod.POST;

@RestController
public class EnvironmentController
{

    @Autowired
    private EnvironmentService environmentService;

    @RequestMapping(value = "/v1/environment/{name}", method = POST)
    public void storeEnvironment(@PathVariable("name") String name, @RequestBody Map<String, String> attributes)
    {
        environmentService.storeEnvironment(name, attributes);
    }

    @RequestMapping(value = "/v1/environment/{name}", method = GET)
    public Environment findEnvironment(@PathVariable("name") String name)
    {
        return environmentService.findEnvironment(name);
    }

    @RequestMapping(value = "/v1/environments/", method = GET)
    public List<Environment> findEnvironment()
    {
        return environmentService.findEnvironments();
    }
}
