/*
 * Copyright 2013-2016, Teradata, Inc. All rights reserved.
 */
package com.teradata.benchto.service.rest;

import com.teradata.benchto.service.TagService;
import com.teradata.benchto.service.model.Tag;
import com.teradata.benchto.service.rest.requests.GetTagsRequest;
import com.teradata.benchto.service.rest.requests.StoreTagRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.ZonedDateTime;
import java.util.List;

import static com.teradata.benchto.service.utils.TimeUtils.currentDateTime;
import static org.springframework.web.bind.annotation.RequestMethod.GET;
import static org.springframework.web.bind.annotation.RequestMethod.POST;

@RestController
public class TagController
{
    @Autowired
    private TagService service;

    @RequestMapping(value = "/v1/tag/{environmentName}", method = POST)
    public void store(@PathVariable("environmentName") String environmentName, @RequestBody StoreTagRequest request)
    {
        service.store(environmentName, request.getName(), request.getDescription());
    }

    @RequestMapping(value = "/v1/tags/{environmentName}", method = GET)
    public List<Tag> find(
            @PathVariable("environmentName") String environmentName,
            @RequestParam(required = false) ZonedDateTime start,
            @RequestParam(required = false) ZonedDateTime end)
    {
        if (start == null) {
            return service.find(environmentName);
        } else {
            end = end != null ? end : currentDateTime();
            return service.find(environmentName, start, end);
        }
    }

    @RequestMapping(value = "/v1/tags/{environmentName}/latest", method = GET)
    public Tag find(
            @PathVariable("environmentName") String environmentName,
            @RequestParam(required = false) ZonedDateTime until)
    {
        if (until == null) {
            until = currentDateTime();
        }
        return service.latest(environmentName, until).orElse(null);
    }
}
