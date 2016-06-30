/*
 * Copyright 2013-2016, Teradata, Inc. All rights reserved.
 */
package com.teradata.benchto.service.repo;

import com.teradata.benchto.service.model.Environment;
import com.teradata.benchto.service.model.Tag;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.ZonedDateTime;
import java.util.List;

@Repository
public interface TagRepo
        extends PagingAndSortingRepository<Tag, String>
{
    @Query(value = "SELECT t FROM Tag t " +
            "WHERE t.environment = :environment " +
            "ORDER BY t.created")
    List<Tag> find(@Param("environment") Environment environment);

    @Query(value = "SELECT t FROM Tag t " +
            "WHERE t.environment = :environment" +
            "   AND t.created >= :startDate " +
            "   AND t.created <= :endDate  " +
            "ORDER BY t.created")
    List<Tag> find(
            @Param("environment") Environment environment,
            @Param("startDate") ZonedDateTime startDate,
            @Param("endDate") ZonedDateTime endDate);

    @Query(value = "SELECT t FROM Tag t " +
            "WHERE t.environment = :environment" +
            "   AND t.created<= :until " +
            "ORDER BY t.created DESC")
    List<Tag> latest(
            @Param("environment") Environment environment,
            @Param("until") ZonedDateTime until,
            Pageable pageable);
}
