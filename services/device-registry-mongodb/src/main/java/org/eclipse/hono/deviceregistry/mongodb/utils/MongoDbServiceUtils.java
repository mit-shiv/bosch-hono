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

import java.util.Optional;

import org.eclipse.hono.deviceregistry.mongodb.model.BaseDto;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;

@SuppressWarnings( {"checkstyle:MissingJavadocType", "checkstyle:HideUtilityClassConstructor"})
/**
 * Utility class for common functions across device registry services.
 */
public final class MongoDbServiceUtils {

    /**
     * Check if a Version is different or not set.
     *
     * @param expectedVersion new version, can unset
     * @param actualValue     current version
     * @return {@code true}, if different version or {@code expectedVersion} is unset
     */
    public static boolean isVersionDifferent(@SuppressWarnings("OptionalUsedAsFieldOrParameterType") final Optional<String> expectedVersion, final String actualValue) {
        return !actualValue.equals(expectedVersion.orElse(actualValue));
    }

    /**
     * Checks if a given version is set and checks compares it to the version of
     *  an {@link BaseDto} entity in the mongodb db found by a given collection and query.
     * 
     * @param mongoClient the mongodb client.
     * @param collection  the mongo db collection to apply the query on.
     * @param query       the query for a {@link BaseDto} JsonObject for version comparision.
     * @param newVersion  the version to compare the queried {@link BaseDto} to. Can be null or {@code Optional.empty()}
     * @return {@code true} if versions of given version and queried BaseDto are equal or no version given, {@code false} else.
     * @throws NullPointerException if no entity could be found with the {@code query}.
     */
    public static Future<Boolean> isVersionEqualCurrentVersionOrNotSet(final MongoClient mongoClient, final String collection, final JsonObject query, final Optional<String> newVersion) {
        if (newVersion == null || !newVersion.isPresent()) {
            return Future.succeededFuture(true);
        }
        final Promise<JsonObject> currentVersionablePromise = Promise.promise();
        mongoClient.findOne(collection, query, new JsonObject(), currentVersionablePromise);
        return currentVersionablePromise.future()
                .compose(currentVersionable -> {
                    if (currentVersionable == null) {
                        throw new NullPointerException("Entity not found.");
                    }
                    final BaseDto currentVersionableDto = currentVersionable.mapTo(BaseDto.class);
                    if (MongoDbServiceUtils.isVersionDifferent(newVersion, currentVersionableDto.getVersion())) {
                        return Future.succeededFuture(false);
                    } else {
                        return Future.succeededFuture(true);
                    }
                });
    }
}
