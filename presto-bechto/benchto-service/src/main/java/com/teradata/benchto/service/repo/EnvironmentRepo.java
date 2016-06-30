/*
 * Copyright 2013-2016, Teradata, Inc. All rights reserved.
 */
package com.teradata.benchto.service.repo;

import com.teradata.benchto.service.model.Environment;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface EnvironmentRepo
        extends JpaRepository<Environment, String>
{
    Environment findByName(String name);
}
