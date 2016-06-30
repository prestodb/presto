/*
 * Copyright 2013-2016, Teradata, Inc. All rights reserved.
 */
package com.teradata.benchto.service.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.MoreObjects;
import org.hibernate.annotations.Type;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

import java.io.Serializable;
import java.time.ZonedDateTime;
import java.util.Objects;

import static javax.persistence.FetchType.LAZY;

@Entity
@Table(name = "tags")
public class Tag
        implements Serializable
{
    @Id
    @SequenceGenerator(name = "tags_id_seq",
            sequenceName = "tags_id_seq",
            allocationSize = 1)
    @GeneratedValue(strategy = GenerationType.SEQUENCE,
            generator = "tags_id_seq")
    @Column(name = "id")
    @JsonIgnore
    private long id;

    @NotNull
    @ManyToOne(fetch = LAZY)
    @JoinColumn(name = "environment_id")
    private Environment environment;

    @Size(min = 1, max = 255)
    @Column(name = "name")
    private String name;

    @Size(min = 0, max = 1024)
    @Column(name = "description")
    private String description;

    @Column(name = "created")
    @Type(type = "org.jadira.usertype.dateandtime.threeten.PersistentZonedDateTime")
    private ZonedDateTime created;

    public String getDescription()
    {
        return description;
    }

    public void setDescription(String description)
    {
        this.description = description;
    }

    public ZonedDateTime getCreated()
    {
        return created;
    }

    public void setCreated(ZonedDateTime created)
    {
        this.created = created;
    }

    public Environment getEnvironment()
    {
        return environment;
    }

    public void setEnvironment(Environment environment)
    {
        this.environment = environment;
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Tag tag1 = (Tag) o;
        return Objects.equals(id, tag1.id) &&
                Objects.equals(environment, tag1.environment) &&
                Objects.equals(name, tag1.name) &&
                Objects.equals(description, tag1.description) &&
                Objects.equals(created, tag1.created);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id, environment, name, description, created);
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                .add("id", id)
                .add("description", description)
                .add("environment", environment)
                .add("name", name)
                .add("created", created)
                .toString();
    }
}
