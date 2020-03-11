/*******************************************************************************
 * Copyright (c) 2020 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information regarding copyright ownership.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0
 *
 * SPDX-License-Identifier: EPL-2.0
 *******************************************************************************/
package org.eclipse.hono.deviceregistry.mongodb.model;

import java.time.Instant;
import java.util.Objects;

import org.eclipse.hono.annotation.HonoTimestamp;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * TODO.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class BaseDto {

    @JsonProperty(value = "version", required = true)
    protected String version;
    @JsonProperty("updatedOn")
    @HonoTimestamp
    protected Instant updatedOn;

    /**
     * Default constructor for serialisation/deserialization.
     */
    public BaseDto() {
        // Explicit default constructor.
    }

    /**
     * Gets the version of the document.
     * 
     * @return The version of the document or {@code null} if not set.
     */
    public String getVersion() {
        return version;
    }

    /**
     * Sets the given version of the document.
     * 
     * @param version The version of the document.
     * @throws NullPointerException if the version is {@code null}.
     */
    public void setVersion(final String version) {
        this.version = Objects.requireNonNull(version);
    }

    /**
     * Gets the date and time of last modification.
     *
     * @return The date and time of last modification.
     */
    public Instant getUpdatedOn() {
        return updatedOn;
    }

    /**
     * Sets the date and time of last modification.
     * 
     * @param updatedOn The date and time of last modification.
     * @throws NullPointerException if the last modification date and time is {@code null}.
     */
    public void setUpdatedOn(final Instant updatedOn) {
        this.updatedOn = Objects.requireNonNull(updatedOn);
    }
}
