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

package org.eclipse.hono.deviceregistry.mongodb.utils;

import org.eclipse.hono.tracing.TracingHelper;
import org.slf4j.Logger;

import com.mongodb.ErrorCategory;
import com.mongodb.MongoException;

import io.opentracing.Span;
import io.vertx.core.Future;

/**
 * Utility class for common error handling functions across device registry services.
 */
@SuppressWarnings("checkstyle:HideUtilityClassConstructor")
public final class MongoDbErrorHandler {

    /**
     * Checks if a provided error is {@link MongoException} with error category {@link ErrorCategory#DUPLICATE_KEY}.
     *
     * @param throwable The exception to check. Other Throwables but {@link MongoException} return {@code false}.
     * @return {@code true} if exception is an {@link ErrorCategory#DUPLICATE_KEY} exception.
     */
    public static boolean ifDuplicateKeyError(final Throwable throwable) {
        if (throwable instanceof MongoException) {
            final MongoException mongoException = (MongoException) throwable;
            return ErrorCategory.fromErrorCode(mongoException.getCode()) == ErrorCategory.DUPLICATE_KEY;
        }
        return false;
    }


    /**
     * A helper method to log error, log message and error on span and return a failed Future for error handling.
     *
     * @param log      current Logger
     * @param span     tracer span, optional
     * @param errorMsg the error message.
     * @param error the thrown error to log on the span.
     * @return Future error containing the message.
     */
    @SuppressWarnings("rawtypes")
    public static Future operationFailed(final Logger log, final Span span, final String errorMsg, final Throwable error) {
        log.error(errorMsg);
        if (span != null) {
            if (error != null) {
                TracingHelper.logError(span, errorMsg, error);
            } else {
                TracingHelper.logError(span, errorMsg);
            }
        }
        return Future.failedFuture(errorMsg);
    }
    /**
     * A helper method to log error, log message on span and return a failed Future for error handling.
     *
     * @param log      current Logger
     * @param span     tracer span, optional
     * @param errorMsg the error message.
     * @return Future error containing the message.
     */
    public static Future operationFailed(final Logger log, final Span span, final String errorMsg) {
        return operationFailed(log, span, errorMsg, null);
    }

}
