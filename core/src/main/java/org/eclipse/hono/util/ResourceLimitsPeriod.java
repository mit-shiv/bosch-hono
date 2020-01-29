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
package org.eclipse.hono.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.OffsetDateTime;
import java.time.YearMonth;
import java.time.temporal.ChronoUnit;
import java.util.Objects;

/**
 * The period definition corresponding to a resource limit for a tenant.
 */
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class ResourceLimitsPeriod {

    @JsonProperty(value = TenantConstants.FIELD_PERIOD_MODE, required = true)
    private String mode;

    @JsonProperty(value = TenantConstants.FIELD_PERIOD_NO_OF_DAYS)
    private int noOfDays;

    /**
     * Gets the mode of period for resource limit calculation.
     *
     * @return The mode of period for resource limit calculation.
     */
    public final String getMode() {
        return mode;
    }

    /**
     * Sets the mode of period for resource limit calculation.
     *
     * @param mode The mode of period for resource limit calculation.
     * @return  a reference to this for fluent use.
     * @throws NullPointerException if mode is {@code null}.
     */
    public final ResourceLimitsPeriod setMode(final String mode) {
        this.mode = Objects.requireNonNull(mode);
        return this;
    }

    /**
     * Gets the number of days for which resource usage is calculated.
     *
     * @return The number of days for a resource limit calculation.
     */
    public final int getNoOfDays() {
        return noOfDays;
    }

    /**
     * Sets the number of days for which resource usage is calculated.
     *
     * @param noOfDays The number of days for which resource usage is calculated.
     * @return  a reference to this for fluent use.
     * @throws IllegalArgumentException if the number of days is negative.
     */
    public final ResourceLimitsPeriod setNoOfDays(final int noOfDays) {
        if (noOfDays < 0) {
            throw new IllegalArgumentException("Number of days property must be  set to value >= 0");
        }
        this.noOfDays = noOfDays;
        return this;
    }

    /**
     * Calculates the period in days for which the resource usage like volume of used data, 
     * connection duration etc. is to be calculated based on the mode defined by 
     * {@link TenantConstants#FIELD_PERIOD_MODE}.
     *
     * @param effectiveSince The point of time on which the resource limit came into effect.
     * @param targetDateTime The target date and time used for the resource usage period calculation.
     * @param mode The mode of the period defined by {@link TenantConstants#FIELD_PERIOD_MODE}.
     * @param periodInDays The number of days defined by {@link TenantConstants#FIELD_PERIOD_NO_OF_DAYS}. 
     * @return The period in days for which the resource usage is to be calculated.
     * @throws NullPointerException if effectiveSince, targetDateTime or mode is {@code null}.
     */
    public static long calculateResourceUsagePeriod(
            final OffsetDateTime effectiveSince,
            final OffsetDateTime targetDateTime,
            final Mode mode,
            final long periodInDays) {

        Objects.requireNonNull(effectiveSince);
        Objects.requireNonNull(targetDateTime);
        Objects.requireNonNull(mode);

        final long inclusiveDaysBetween = ChronoUnit.DAYS.between(effectiveSince, targetDateTime) + 1;

        switch (mode) {
        case DAYS:
            if (inclusiveDaysBetween > 0 && periodInDays > 0) {
                final long dataUsagePeriodInDays = inclusiveDaysBetween % periodInDays;
                return dataUsagePeriodInDays == 0 ? periodInDays : dataUsagePeriodInDays;
            }
            return 0L;
        case MONTHLY:
            if (YearMonth.from(targetDateTime).equals(YearMonth.from(effectiveSince))
                    && effectiveSince.getDayOfMonth() != 1) {
                return inclusiveDaysBetween;
            }
            return targetDateTime.getDayOfMonth();
        default:
            return 0L;
        }
    }

    /**
     * The mode of the resource limit period.
     *
     */
    public enum Mode {
        /**
         * The days mode.
         */
        DAYS("days"),
        /**
         * The monthly mode.
         */
        MONTHLY("monthly"),
        /**
         * The mode is unknown.
         */
        UNKNOWN("unknown");

        private final String mode;

        Mode(final String mode) {
            this.mode = mode;
        }

        /**
         * Construct a PeriodMode from a given value.
         *
         * @param value The value from which the PeriodMode needs to be constructed.
         * @return The PeriodMode as enum
         */
        public static Mode from(final String value) {
            if (value != null) {
                for (Mode mode : values()) {
                    if (value.equalsIgnoreCase(mode.value())) {
                        return mode;
                    }
                }
            }
            return UNKNOWN;
        }

        /**
         * Gets the string equivalent of the mode.
         *
         * @return The value of the mode.
         */
        public String value() {
            return this.mode;
        }
    }
}
