/*
 * Copyright 2013-2016, Teradata, Inc. All rights reserved.
 */
package com.teradata.benchto.service.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.hibernate.annotations.BatchSize;
import org.hibernate.annotations.Cache;
import org.hibernate.annotations.Type;

import javax.persistence.Cacheable;
import javax.persistence.CascadeType;
import javax.persistence.CollectionTable;
import javax.persistence.Column;
import javax.persistence.ElementCollection;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.ManyToOne;
import javax.persistence.MapKeyColumn;
import javax.persistence.OneToMany;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;
import javax.persistence.Version;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

import java.io.Serializable;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.Maps.newHashMap;
import static javax.persistence.FetchType.EAGER;
import static org.hibernate.annotations.CacheConcurrencyStrategy.TRANSACTIONAL;

@Cacheable
@Entity
@Table(name = "executions")
public class BenchmarkRunExecution
        implements Serializable
{

    @Id
    @SequenceGenerator(name = "executions_id_seq",
            sequenceName = "executions_id_seq",
            allocationSize = 1)
    @GeneratedValue(strategy = GenerationType.SEQUENCE,
            generator = "executions_id_seq")
    @Column(name = "id")
    @JsonIgnore
    private long id;

    @Size(min = 1, max = 64)
    @Column(name = "sequence_id")
    private String sequenceId;

    @JsonIgnore
    @Column(name = "version")
    @Version
    private Long version;

    @Enumerated(EnumType.STRING)
    @Column(name = "status")
    private Status status;

    @NotNull
    @JsonIgnore
    @ManyToOne
    private BenchmarkRun benchmarkRun;

    @BatchSize(size = 10)
    @OneToMany(cascade = CascadeType.ALL, fetch = EAGER)
    @JoinTable(name = "execution_measurements",
            joinColumns = @JoinColumn(name = "execution_id", referencedColumnName = "id"),
            inverseJoinColumns = @JoinColumn(name = "measurement_id", referencedColumnName = "id"))
    private Set<Measurement> measurements;

    @Column(name = "started")
    @Type(type = "org.jadira.usertype.dateandtime.threeten.PersistentZonedDateTime")
    private ZonedDateTime started;

    @Column(name = "ended")
    @Type(type = "org.jadira.usertype.dateandtime.threeten.PersistentZonedDateTime")
    private ZonedDateTime ended;

    @Cache(usage = TRANSACTIONAL)
    @BatchSize(size = 10)
    @ElementCollection(fetch = EAGER)
    @MapKeyColumn(name = "name")
    @Column(name = "value")
    @CollectionTable(name = "execution_attributes", joinColumns = @JoinColumn(name = "execution_id"))
    private Map<String, String> attributes = newHashMap();

    public long getId()
    {
        return id;
    }

    public void setId(long id)
    {
        this.id = id;
    }

    public String getSequenceId()
    {
        return sequenceId;
    }

    public void setSequenceId(String sequenceId)
    {
        this.sequenceId = sequenceId;
    }

    public Status getStatus()
    {
        return status;
    }

    public void setStatus(Status status)
    {
        this.status = status;
    }

    public Long getVersion()
    {
        return version;
    }

    public void setVersion(Long version)
    {
        this.version = version;
    }

    public Set<Measurement> getMeasurements()
    {
        return measurements;
    }

    public void setMeasurements(Set<Measurement> measurements)
    {
        this.measurements = measurements;
    }

    public ZonedDateTime getStarted()
    {
        return started;
    }

    public void setStarted(ZonedDateTime started)
    {
        this.started = started;
    }

    public ZonedDateTime getEnded()
    {
        return ended;
    }

    public void setEnded(ZonedDateTime ended)
    {
        this.ended = ended;
    }

    public BenchmarkRun getBenchmarkRun()
    {
        return benchmarkRun;
    }

    public void setBenchmarkRun(BenchmarkRun benchmarkRun)
    {
        this.benchmarkRun = benchmarkRun;
    }

    public Map<String, String> getAttributes()
    {
        return attributes;
    }

    public void setAttributes(Map<String, String> attributes)
    {
        this.attributes = attributes;
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
        BenchmarkRunExecution execution = (BenchmarkRunExecution) o;
        return Objects.equals(sequenceId, execution.sequenceId);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(sequenceId);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("id", id)
                .add("sequenceId", sequenceId)
                .add("status", status)
                .add("version", version)
                .add("measurements", measurements)
                .add("attributes", attributes)
                .add("started", started)
                .add("ended", ended)
                .toString();
    }
}
