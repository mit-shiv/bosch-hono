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

import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Verifies the behavior of {@link ResourceLimitsPeriod}.
 */
public class ResourceLimitsPeriodTest {

    /**
     * Verifies the resource usage period calculation for <em>monthly</em> mode.
     */
    @Test
    public void verifyResourceUsagePeriodCalculationForMonthlyMode() {
        // The case where the effectiveSince lies on the past months of the target date.
        assertEquals(6,
                ResourceLimitsPeriod.calculateResourceUsagePeriod(
                        OffsetDateTime.parse("2019-08-06T14:30:00Z", DateTimeFormatter.ISO_OFFSET_DATE_TIME),
                        OffsetDateTime.parse("2019-09-06T14:30:00Z", DateTimeFormatter.ISO_OFFSET_DATE_TIME),
                        ResourceLimitsPeriod.Mode.MONTHLY,
                        0));
        // The case where the effectiveSince lies on the the same month as of the target date.
        assertEquals(5,
                ResourceLimitsPeriod.calculateResourceUsagePeriod(
                        OffsetDateTime.parse("2019-09-06T14:30:00Z", DateTimeFormatter.ISO_OFFSET_DATE_TIME),
                        OffsetDateTime.parse("2019-09-10T14:30:00Z", DateTimeFormatter.ISO_OFFSET_DATE_TIME),
                        ResourceLimitsPeriod.Mode.MONTHLY,
                        0));
    }

    /**
     * Verifies the resource usage period calculation for <em>days</em> mode.
     */
    @Test
    public void verifyResourceUsagePeriodCalculationForDaysMode() {
        final long noOfDays = 30;
        // The case where the effectiveSince lies on the past months of the target date.
        assertEquals(6,
                ResourceLimitsPeriod.calculateResourceUsagePeriod(
                        OffsetDateTime.parse("2019-08-06T14:30:00Z", DateTimeFormatter.ISO_OFFSET_DATE_TIME),
                        OffsetDateTime.parse("2019-09-10T14:30:00Z", DateTimeFormatter.ISO_OFFSET_DATE_TIME),
                        ResourceLimitsPeriod.Mode.DAYS,
                        noOfDays));
        // The case where the effectiveSince lies on the the same month as of the target date.
        assertEquals(5,
                ResourceLimitsPeriod.calculateResourceUsagePeriod(
                        OffsetDateTime.parse("2019-09-06T14:30:00Z", DateTimeFormatter.ISO_OFFSET_DATE_TIME),
                        OffsetDateTime.parse("2019-09-10T14:30:00Z", DateTimeFormatter.ISO_OFFSET_DATE_TIME),
                        ResourceLimitsPeriod.Mode.DAYS,
                        noOfDays));
    }
}
