/**
 * Copyright (c) 2017 Bosch Software Innovations GmbH.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * Contributors:
 *    Bosch Software Innovations GmbH - initial creation
 */
package org.eclipse.hono.service.credentials;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.eclipse.hono.util.CredentialsConstants.FIELD_SECRETS;

import org.eclipse.hono.config.ServiceConfigProperties;
import org.eclipse.hono.util.CredentialsResult;
import org.junit.Before;
import org.junit.Test;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * Unit tests for BaseCredentialsService
 *
 */
public class BaseCredentialsServiceTest {

    private BaseCredentialsService<ServiceConfigProperties> service;

    private static final String TEST_FIELD = "dummy_test";

    @Before
    public void setUp() {
        this.service = buildBaseCredentialsServiceDummy();
    }

    @Test
    public void testIsValidTimestampForFieldWithValidTimestamps() {
        String iso8601TimeStamp = "2007-04-05T12:30-02:00";

        JsonObject testData = new JsonObject();
        testData.put(TEST_FIELD, iso8601TimeStamp);
        boolean result = service.isValidTimestampForField(testData, TEST_FIELD);
        assertTrue(result);

        String shortIso8601TimeStamp = "2007-04-05T14:30";
        testData.put(TEST_FIELD, shortIso8601TimeStamp);
        result = service.isValidTimestampForField(testData, TEST_FIELD);
        assertTrue(result);
    }

    @Test
    public void testIsValidTimestampForFieldWithInValidTimestamp() {
        String iso8601TimeStamp = "yakshaver";

        JsonObject testData = new JsonObject();
        testData.put(TEST_FIELD, iso8601TimeStamp);

        boolean result = service.isValidTimestampForField(testData, TEST_FIELD);
        assertFalse(result);
    }

    @Test
    public void testContainsValidTimestampIfPresentForFieldWhenNotPresent() {
        JsonObject testData = new JsonObject();

        boolean valid = service.containsValidTimestampIfPresentForField(testData, TEST_FIELD);
        assertTrue(valid);
    }

    @Test
    public void testContainsValidTimestampIfPresentForFieldWhenPresentValid() {
        JsonObject testData = new JsonObject();
        testData.put(TEST_FIELD, "2007-04-05T14:30");

        boolean valid = service.containsValidTimestampIfPresentForField(testData, TEST_FIELD);
        assertTrue(valid);
    }

    @Test
    public void testContainsValidTimestampIfPresentForFieldWhenPresentInvalid() {
        JsonObject testData = new JsonObject();
        testData.put(TEST_FIELD, "yakshaver");

        boolean valid = service.containsValidTimestampIfPresentForField(testData, TEST_FIELD);
        assertFalse(valid);
    }

    @Test
    public void testContainsStringValueForFieldWhenNotPresent() {
        JsonObject testData = new JsonObject();

        boolean valid = service.containsStringValueForField(testData, TEST_FIELD);
        assertFalse(valid);
    }

    @Test
    public void testContainsStringValueForFieldWhenPresent() {
        JsonObject testData = new JsonObject();
        testData.put(TEST_FIELD, "yakshaver");

        boolean valid = service.containsStringValueForField(testData, TEST_FIELD);
        assertTrue(valid);
    }

    @Test
    public void testContainsStringValueForFieldWhenPresentEmpty() {
        JsonObject testData = new JsonObject();
        testData.put(TEST_FIELD, "");

        boolean valid = service.containsStringValueForField(testData, TEST_FIELD);
        assertFalse(valid);
    }

    @Test
    public void testContainsValidSecretValueWithMissingSecret() {
        JsonObject testData = new JsonObject();

        boolean valid = service.containsValidSecretValue(testData);
        assertFalse(valid);
    }

    @Test
    public void testContainsValidSecretValueWithNoSecrets() {
        JsonObject testData = new JsonObject();
        JsonArray testSecrets = new JsonArray();
        testData.put(FIELD_SECRETS, testSecrets);
        
        boolean valid = service.containsValidSecretValue(testData);
        assertTrue(valid);
    }

    @Test
    public void testContainsValidSecretValueWithEmptySecret() {
        JsonObject testData = new JsonObject();
        JsonArray testSecrets = new JsonArray();
        JsonObject testSecret = new JsonObject();
        testSecrets.add(testSecret);
        testData.put(FIELD_SECRETS, testSecrets);

        boolean valid = service.containsValidSecretValue(testData);
        assertTrue(valid);
    }

    @Test
    public void testContainsValidSecretValueWithValidSecret() {
        JsonObject testData = new JsonObject();
        JsonArray testSecrets = new JsonArray();
        JsonObject testSecret = new JsonObject();
        testSecret.put("key", "myPwd");
        testSecrets.add(testSecret);
        testData.put(FIELD_SECRETS, testSecrets);

        boolean valid = service.containsValidSecretValue(testData);
        assertTrue(valid);
    }

    private BaseCredentialsService<ServiceConfigProperties> buildBaseCredentialsServiceDummy() {
        return new BaseCredentialsService<ServiceConfigProperties>() {
            @Override
            public void updateCredentials(String tenantId, JsonObject credentialsObject,
                    Handler<AsyncResult<CredentialsResult>> resultHandler) {
            }
            @Override
            public void removeCredentials(String tenantId, String deviceId, String type, String authId,
                    Handler<AsyncResult<CredentialsResult>> resultHandler) {
            }
            @Override
            public void getCredentials(String tenantId, String type, String authId,
                    Handler<AsyncResult<CredentialsResult>> resultHandler) {
            }
            @Override
            public void addCredentials(String tenantId, JsonObject credentialsObject,
                    Handler<AsyncResult<CredentialsResult>> resultHandler) {
            }
            @Override
            public void setConfig(ServiceConfigProperties configuration) {
            }
        };
    }
}
