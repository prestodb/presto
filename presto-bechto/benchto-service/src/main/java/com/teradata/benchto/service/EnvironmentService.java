/*
 * Copyright 2013-2016, Teradata, Inc. All rights reserved.
 */
package com.teradata.benchto.service;

import com.teradata.benchto.service.model.Environment;
import com.teradata.benchto.service.repo.EnvironmentRepo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.teradata.benchto.service.utils.TimeUtils.currentDateTime;
import static java.util.Optional.ofNullable;

@Service
public class EnvironmentService
{
    private static final Logger LOG = LoggerFactory.getLogger(EnvironmentService.class);

    @Autowired
    private EnvironmentRepo environmentRepo;

    @Retryable(value = {TransientDataAccessException.class, DataIntegrityViolationException.class}, maxAttempts = 1)
    @Transactional
    public void storeEnvironment(String name, Map<String, String> attributes)
    {
        Optional<Environment> environmentOptional = tryFindEnvironment(name);
        if (!environmentOptional.isPresent()) {
            Environment environment = new Environment();
            environment.setName(name);
            environment.setAttributes(attributes);
            environment.setStarted(currentDateTime());
            environmentOptional = Optional.of(environmentRepo.save(environment));
        } else {
            environmentOptional.get().setAttributes(attributes);
        }
        LOG.debug("Starting environment - {}", environmentOptional.get());
    }

    @Transactional(readOnly = true)
    public Environment findEnvironment(String name)
    {
        Optional<Environment> environment = tryFindEnvironment(name);
        if (!environment.isPresent()) {
            throw new IllegalArgumentException("Could not find environment " + name);
        }
        return environment.get();
    }

    @Transactional(readOnly = true)
    public Optional<Environment> tryFindEnvironment(String name)
    {
        return ofNullable(environmentRepo.findByName(name));
    }

    public List<Environment> findEnvironments()
    {
        return environmentRepo.findAll();
    }
}
