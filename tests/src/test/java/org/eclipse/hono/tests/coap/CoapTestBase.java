/*******************************************************************************
 * Copyright (c) 2019, 2020 Contributors to the Eclipse Foundation
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

package org.eclipse.hono.tests.coap;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.HttpURLConnection;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.jms.IllegalStateException;

import org.apache.qpid.proton.message.Message;
import org.assertj.core.data.Index;
import org.eclipse.californium.core.coap.MediaTypeRegistry;
import org.eclipse.californium.core.coap.OptionSet;
import org.eclipse.hono.client.MessageConsumer;
import org.eclipse.hono.client.ServiceInvocationException;
import org.eclipse.hono.service.management.device.Device;
import org.eclipse.hono.service.management.tenant.Tenant;
import org.eclipse.hono.tests.ClientDevice;
import org.eclipse.hono.tests.IntegrationTestSupport;
import org.eclipse.hono.util.Adapter;
import org.eclipse.hono.util.CommandConstants;
import org.eclipse.hono.util.Constants;
import org.eclipse.hono.util.MessageHelper;
import org.eclipse.hono.util.TimeUntilDisconnectNotification;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;

/**
 * Base class for CoAP adapter integration tests.
 *
 */
public abstract class CoapTestBase {

    /**
     * The default password of devices.
     */
    protected static final String SECRET = "secret";

    /**
     * A helper for accessing the AMQP 1.0 Messaging Network and
     * for managing tenants/devices/credentials.
     */
    protected static IntegrationTestSupport helper;
    /**
     * The period of time in milliseconds after which test cases should time out.
     */
    protected static final long TEST_TIMEOUT_MILLIS = 20000; // 20 seconds

    protected static final Vertx VERTX = Vertx.vertx();

    private static final int MESSAGES_TO_SEND = 60;

    private static final String COMMAND_TO_SEND = "setDarkness";
    private static final String COMMAND_JSON_KEY = "darkness";

    /**
     * A logger to be shared with subclasses.
     */
    protected final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * The random tenant identifier created for each test case.
     */
    protected String tenantId;
    /**
     * The random device identifier created for each test case.
     */
    protected String deviceId;

    /**
     * Sets up clients.
     * 
     * @param ctx The vert.x test context.
     */
    @BeforeAll
    public static void init(final VertxTestContext ctx) {

        helper = new IntegrationTestSupport(VERTX);
        helper.init().setHandler(ctx.completing());
    }

    /**
     * Asserts the status code of a failed CoAP request.
     * 
     * @param expectedStatus The expected status.
     * @param t The exception to verify.
     * @throws AssertionError if any of the checks fail.
     */
    protected static void assertServiceInvocationErrorCode(final int expectedStatus, final Throwable t) {
        assertThat(t).isInstanceOf(ServiceInvocationException.class);
        assertThat(((ServiceInvocationException) t).getErrorCode()).isEqualTo(expectedStatus);
    }

    /**
     * Sets up the fixture.
     * 
     * @param testInfo The test meta data.
     * @throws UnknownHostException if the CoAP adapter's host name cannot be resolved.
     */
    @BeforeEach
    public void setUp(final TestInfo testInfo) throws UnknownHostException {

        logger.info("running {}", testInfo.getDisplayName());
        logger.info("using CoAP adapter [host: {}, coap port: {}, coaps port: {}]",
                IntegrationTestSupport.COAP_HOST,
                IntegrationTestSupport.COAP_PORT,
                IntegrationTestSupport.COAPS_PORT);

        tenantId = helper.getRandomTenantId();
        deviceId = helper.getRandomDeviceId(tenantId);
    }

    /**
     * Deletes all temporary objects from the Device Registry which
     * have been created during the last test execution.
     * 
     * @param ctx The vert.x context.
     */
    @AfterEach
    public void deleteObjects(final VertxTestContext ctx) {

        helper.deleteObjects(ctx);
    }

    /**
     * Closes the AMQP 1.0 Messaging Network client.
     * 
     * @param ctx The vert.x test context.
     */
    @AfterAll
    public static void disconnect(final VertxTestContext ctx) {

        helper.disconnect().setHandler(ctx.completing());
    }

    /**
     * Triggers the downstream links in the CoAP adapter to be established
     * by sending a <em>warm-up</em> message.
     * 
     * @param device The device to use for warming up.
     * @param originDeviceId The identifier of the device that the data originates from.
     * @return A future indicating the outcome.
     */
    protected Future<?> warmUp(final ClientDevice<OptionSet, OptionSet> device, final String originDeviceId) {
        logger.debug("sending request to trigger CoAP adapter's downstream message sender");
        final Promise<Void> result = Promise.promise();
        final OptionSet options = new OptionSet().setContentFormat(MediaTypeRegistry.TEXT_PLAIN);
        device.uploadData(originDeviceId, options, Buffer.buffer("warm-up"))
            .setHandler(r -> {
                if (r.succeeded()) {
                    VERTX.setTimer(500, tid -> result.complete());
                } else {
                    result.fail(r.cause());
                }
            });
        return result.future();
    }

    /**
     * Creates a test specific message consumer.
     *
     * @param tenantId        The tenant to create the consumer for.
     * @param messageConsumer The handler to invoke for every message received.
     * @return A future succeeding with the created consumer.
     */
    protected abstract Future<MessageConsumer> createConsumer(String tenantId, Consumer<Message> messageConsumer);

    /**
     * Verifies that a number of messages uploaded to Hono's CoAP adapter using TLS_PSK based authentication can be
     * successfully consumed via the AMQP Messaging Network.
     * 
     * @param endpointConfig The endpoints to use for uploading messages.
     * @param ctx The test context.
     * @throws InterruptedException if the test fails.
     */
    @ParameterizedTest(name = IntegrationTestSupport.PARAMETERIZED_TEST_NAME_PATTERN)
    @MethodSource("deviceTelemetryVariants")
    public void testUploadMessages(
            final CoapCommandEndpointConfiguration endpointConfig,
            final VertxTestContext ctx) throws InterruptedException {

        final Tenant tenant = new Tenant();

        final VertxTestContext setup = new VertxTestContext();
        helper.registry.addPskDeviceForTenant(tenantId, tenant, deviceId, SECRET).setHandler(setup.completing());
        ctx.verify(() -> assertThat(setup.awaitCompletion(5, TimeUnit.SECONDS)).isTrue());

        final CoapDevice device = new CoapDevice(tenantId, deviceId, SECRET, endpointConfig);
        final OptionSet options = new OptionSet().setContentFormat(MediaTypeRegistry.TEXT_PLAIN);

        testUploadMessages(ctx, tenantId,
                () -> warmUp(device, deviceId),
                messageCount -> device.uploadData(deviceId, options, Buffer.buffer("hello " + messageCount)));
    }

    /**
     * Verifies that a number of messages uploaded to the CoAP adapter via a gateway
     * using TLS_PSK can be successfully consumed via the AMQP Messaging Network.
     * 
     * @param endpointConfig The endpoints to use for uploading messages.
     * @param ctx The test context.
     * @throws InterruptedException if the test fails.
     */
    @ParameterizedTest(name = IntegrationTestSupport.PARAMETERIZED_TEST_NAME_PATTERN)
    @MethodSource("gatewayTelemetryVariants")
    public void testUploadMessagesViaGateway(
            final CoapCommandEndpointConfiguration endpointConfig,
            final VertxTestContext ctx) throws InterruptedException {

        // GIVEN a device that is connected via two gateways
        final Tenant tenant = new Tenant();
        final String gatewayOneId = helper.getRandomDeviceId(tenantId);
        final String gatewayTwoId = helper.getRandomDeviceId(tenantId);
        final Device deviceData = new Device();
        deviceData.setVia(List.of(gatewayOneId, gatewayTwoId));

        final VertxTestContext setup = new VertxTestContext();
        helper.registry.addPskDeviceForTenant(tenantId, tenant, gatewayOneId, SECRET)
            .compose(ok -> helper.registry.addPskDeviceToTenant(tenantId, gatewayTwoId, SECRET))
            .compose(ok -> helper.registry.registerDevice(tenantId, deviceId, deviceData))
            .setHandler(setup.completing());
        ctx.verify(() -> assertThat(setup.awaitCompletion(5, TimeUnit.SECONDS)).isTrue());

        final CoapDevice gatewayOne = new CoapDevice(tenantId, gatewayOneId, SECRET, endpointConfig);
        final CoapDevice gatewayTwo = new CoapDevice(tenantId, gatewayTwoId, SECRET, endpointConfig);
        final OptionSet options = new OptionSet().setContentFormat(MediaTypeRegistry.TEXT_PLAIN);

        testUploadMessages(ctx, tenantId,
                () -> warmUp(gatewayOne, deviceId),
                count -> {
                    final CoapDevice gw = (count.intValue() & 1) == 0 ? gatewayOne : gatewayTwo;
                    return gw.uploadData(deviceId, options, Buffer.buffer("hello " + count));
                });
    }

    /**
     * Uploads messages to the CoAP endpoint.
     *
     * @param ctx The test context to run on.
     * @param tenantId The tenant that the device belongs to.
     * @param warmUp A sender of messages used to warm up the adapter before
     *               running the test itself or {@code null} if no warm up should
     *               be performed. 
     * @param requestSender The test device that will publish the data.
     * @throws InterruptedException if the test is interrupted before it
     *              has finished.
     */
    protected void testUploadMessages(
            final VertxTestContext ctx,
            final String tenantId,
            final Supplier<Future<?>> warmUp,
            final Function<Integer, Future<OptionSet>> requestSender) throws InterruptedException {
        testUploadMessages(ctx, tenantId, warmUp, null, requestSender);
    }

    /**
     * Uploads messages to the CoAP endpoint.
     *
     * @param ctx The test context to run on.
     * @param tenantId The tenant that the device belongs to.
     * @param messageConsumer Consumer that is invoked when a message was received.
     * @param warmUp A sender of messages used to warm up the adapter before
     *               running the test itself or {@code null} if no warm up should
     *               be performed. 
     * @param requestSender The test device that will publish the data.
     * @throws InterruptedException if the test is interrupted before it
     *              has finished.
     */
    protected void testUploadMessages(
            final VertxTestContext ctx,
            final String tenantId,
            final Supplier<Future<?>> warmUp,
            final Consumer<Message> messageConsumer,
            final Function<Integer, Future<OptionSet>> requestSender) throws InterruptedException {
        testUploadMessages(ctx, tenantId, warmUp, messageConsumer, requestSender, MESSAGES_TO_SEND);
    }

    /**
     * Uploads messages to the CoAP endpoint.
     *
     * @param ctx The test context to run on.
     * @param tenantId The tenant that the device belongs to.
     * @param warmUp A sender of messages used to warm up the adapter before running the test itself or {@code null} if
     *            no warm up should be performed.
     * @param messageConsumer Consumer that is invoked when a message was received.
     * @param requestSender The test device that will publish the data.
     * @param numberOfMessages The number of messages that are uploaded.
     * @throws InterruptedException if the test is interrupted before it has finished.
     */
    protected void testUploadMessages(
            final VertxTestContext ctx,
            final String tenantId,
            final Supplier<Future<?>> warmUp,
            final Consumer<Message> messageConsumer,
            final Function<Integer, Future<OptionSet>> requestSender,
            final int numberOfMessages) throws InterruptedException {

        final CountDownLatch received = new CountDownLatch(numberOfMessages);

        final VertxTestContext setup = new VertxTestContext();
        createConsumer(tenantId, msg -> {
            logger.trace("received {}", msg);
            assertMessageProperties(ctx, msg);
            if (messageConsumer != null) {
                messageConsumer.accept(msg);
            }
            received.countDown();
            if (received.getCount() % 20 == 0) {
                logger.info("messages received: {}", numberOfMessages - received.getCount());
            }
        })
        .compose(ok -> Optional.ofNullable(warmUp).map(w -> w.get()).orElse(Future.succeededFuture()))
        .setHandler(setup.completing());
        ctx.verify(() -> assertThat(setup.awaitCompletion(5, TimeUnit.SECONDS)).isTrue());

        final long start = System.currentTimeMillis();
        final AtomicInteger messageCount = new AtomicInteger(0);

        while (messageCount.get() < numberOfMessages) {

            final CountDownLatch sending = new CountDownLatch(1);
            requestSender.apply(messageCount.getAndIncrement()).compose(this::assertCoapResponse)
                    .setHandler(attempt -> {
                        if (attempt.succeeded()) {
                            logger.debug("sent message {}", messageCount.get());
                        } else {
                            logger.info("failed to send message {}: {}", messageCount.get(),
                                    attempt.cause().getMessage());
                            ctx.failNow(attempt.cause());
                        }
                        sending.countDown();;
                    });

            if (messageCount.get() % 20 == 0) {
                logger.info("messages sent: {}", messageCount.get());
            }
            sending.await();
        }

        final long timeToWait = Math.max(TEST_TIMEOUT_MILLIS - 1000, Math.round(numberOfMessages * 20));
        if (received.await(timeToWait, TimeUnit.MILLISECONDS)) {
            logger.info("sent {} and received {} messages after {} milliseconds",
                    messageCount, numberOfMessages - received.getCount(), System.currentTimeMillis() - start);
            ctx.completeNow();
        } else {
            logger.info("sent {} and received {} messages after {} milliseconds",
                    messageCount, numberOfMessages - received.getCount(), System.currentTimeMillis() - start);
            ctx.failNow(new AssertionError("did not receive all messages sent"));
        }
    }

    /**
     * Verifies that the upload of a telemetry message containing a payload that
     * exceeds the CoAP adapter's configured max payload size fails with a 4.13
     * response code.
     * 
     * @param endpointConfig The endpoints to use for uploading messages.
     * @param ctx The test context.
     */
    @ParameterizedTest(name = IntegrationTestSupport.PARAMETERIZED_TEST_NAME_PATTERN)
    @MethodSource("deviceTelemetryVariants")
    @Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
    public void testUploadMessageFailsForLargePayload(
            final CoapCommandEndpointConfiguration endpointConfig,
            final VertxTestContext ctx) {

        final Tenant tenant = new Tenant();

        createConsumer(tenantId, msg -> {
            ctx.failNow(new IllegalStateException("consumer should not have received message"));
        })
        .compose(consumer -> helper.registry.addPskDeviceForTenant(tenantId, tenant, deviceId, SECRET))
        .compose(ok -> {
            final CoapDevice device = new CoapDevice(tenantId, deviceId, SECRET, endpointConfig);
            final OptionSet options = new OptionSet().setContentFormat(MediaTypeRegistry.TEXT_PLAIN);
            return device.uploadData(deviceId, options, Buffer.buffer(IntegrationTestSupport.getPayload(4096)));
        })
        .setHandler(ctx.failing(t -> {
            // THEN the request fails because the request entity is too long
            ctx.verify(() -> assertServiceInvocationErrorCode(HttpURLConnection.HTTP_ENTITY_TOO_LARGE, t));
            ctx.completeNow();
        }));
    }

    /**
     * Verifies that the adapter fails to authenticate a device if the shared key registered
     * for the device does not match the key used by the device in the DTLS handshake.
     * 
     * @param endpointConfig The endpoints to use for uploading messages.
     * @param ctx The vert.x test context.
     */
    @ParameterizedTest(name = IntegrationTestSupport.PARAMETERIZED_TEST_NAME_PATTERN)
    @MethodSource("authenticatedDeviceTelemetryVariants")
    @Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
    public void testUploadFailsForNonMatchingSharedKey(
            final CoapCommandEndpointConfiguration endpointConfig,
            final VertxTestContext ctx) {

        final Tenant tenant = new Tenant();

        // GIVEN a device for which PSK credentials have been registered
        createConsumer(tenantId, msg -> {
            ctx.failNow(new IllegalStateException("consumer should not have received message"));
        })
        .compose(consumer -> helper.registry.addPskDeviceForTenant(tenantId, tenant, deviceId, SECRET))
        .compose(ok -> {
            // WHEN a device tries to upload data and authenticate using the PSK
            // identity for which the server has a different shared secret on record
            final CoapDevice device = new CoapDevice(tenantId, deviceId, "WRONG" + SECRET, endpointConfig);
            final OptionSet options = new OptionSet().setContentFormat(MediaTypeRegistry.TEXT_PLAIN);
            return device.uploadData(deviceId, options, Buffer.buffer("hello"));
        })
        .setHandler(ctx.failing(t -> {
            // THEN the request fails because the DTLS handshake cannot be completed
            ctx.verify(() -> assertServiceInvocationErrorCode(HttpURLConnection.HTTP_UNAVAILABLE, t));
            ctx.completeNow();
        }));
    }

    /**
     * Verifies that the CoAP adapter rejects messages from a device that belongs to a tenant for which the CoAP adapter
     * has been disabled.
     *
     * @param endpointConfig The endpoints to use for uploading messages.
     * @param ctx The test context
     */
    @ParameterizedTest(name = IntegrationTestSupport.PARAMETERIZED_TEST_NAME_PATTERN)
    @MethodSource("deviceTelemetryVariants")
    @Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
    public void testUploadMessageFailsForDisabledTenant(
            final CoapCommandEndpointConfiguration endpointConfig,
            final VertxTestContext ctx) {

        // GIVEN a tenant for which the CoAP adapter is disabled
        final Tenant tenant = new Tenant();
        tenant.addAdapterConfig(new Adapter(Constants.PROTOCOL_ADAPTER_TYPE_COAP).setEnabled(false));

        helper.registry.addPskDeviceForTenant(tenantId, tenant, deviceId, SECRET)
            .compose(ok -> {

                // WHEN a device that belongs to the tenant uploads a message
                final CoapDevice device = new CoapDevice(tenantId, deviceId, SECRET, endpointConfig);
                final OptionSet options = new OptionSet().setContentFormat(MediaTypeRegistry.TEXT_PLAIN);
                return device.uploadData(deviceId, options, Buffer.buffer("hello"));
            })
            .setHandler(ctx.failing(t -> {
                // THEN the request fails
                ctx.verify(() -> assertServiceInvocationErrorCode(HttpURLConnection.HTTP_FORBIDDEN, t));
                ctx.completeNow();
            }));
    }

    /**
     * Verifies that the CoAP adapter rejects messages from a disabled device.
     *
     * @param endpointConfig The endpoints to use for uploading messages.
     * @param ctx The test context
     */
    @ParameterizedTest(name = IntegrationTestSupport.PARAMETERIZED_TEST_NAME_PATTERN)
    @MethodSource("deviceTelemetryVariants")
    @Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
    public void testUploadMessageFailsForDisabledDevice(
            final CoapCommandEndpointConfiguration endpointConfig,
            final VertxTestContext ctx) {

        // GIVEN a disabled device
        final Tenant tenant = new Tenant();
        final Device deviceData = new Device();
        deviceData.setEnabled(false);

        helper.registry.addPskDeviceForTenant(tenantId, tenant, deviceId, deviceData, SECRET)
            .compose(ok -> {

                // WHEN the device tries to upload a message
                final CoapDevice device = new CoapDevice(tenantId, deviceId, SECRET, endpointConfig);
                final OptionSet options = new OptionSet().setContentFormat(MediaTypeRegistry.TEXT_PLAIN);
                return device.uploadData(deviceId, options, Buffer.buffer("hello"));
            })
            .setHandler(ctx.failing(t -> {
                // THEN the request fails
                ctx.verify(() -> assertServiceInvocationErrorCode(HttpURLConnection.HTTP_NOT_FOUND, t));
                ctx.completeNow();
            }));
    }

    /**
     * Verifies that the CoAP adapter rejects messages from a disabled gateway
     * for an enabled device with a 403.
     *
     * @param endpointConfig The endpoints to use for uploading messages.
     * @param ctx The test context
     */
    @ParameterizedTest(name = IntegrationTestSupport.PARAMETERIZED_TEST_NAME_PATTERN)
    @MethodSource("authenticatedGatewayTelemetryVariants")
    @Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
    public void testUploadMessageFailsForDisabledGateway(
            final CoapCommandEndpointConfiguration endpointConfig,
            final VertxTestContext ctx) {

        // GIVEN a device that is connected via a disabled gateway
        final Tenant tenant = new Tenant();
        final String gatewayId = helper.getRandomDeviceId(tenantId);
        final Device gatewayData = new Device().setEnabled(false);
        final Device deviceData = new Device().setVia(List.of(gatewayId));

        createConsumer(tenantId, msg -> {
            ctx.failNow(new IllegalStateException("consumer should not have received message"));
        })
        .compose(consumer -> helper.registry.addPskDeviceForTenant(tenantId, tenant, gatewayId, gatewayData, SECRET))
        .compose(ok -> helper.registry.registerDevice(tenantId, deviceId, deviceData))
        .compose(ok -> {

            // WHEN the gateway tries to upload a message for the device
            final CoapDevice gw = new CoapDevice(tenantId, gatewayId, SECRET, endpointConfig);
            final OptionSet options = new OptionSet().setContentFormat(MediaTypeRegistry.TEXT_PLAIN);
            return gw.uploadData(deviceId, options, Buffer.buffer("hello"));
        })
        .setHandler(ctx.failing(t -> {
            // THEN the request fails
            ctx.verify(() -> assertServiceInvocationErrorCode(HttpURLConnection.HTTP_FORBIDDEN, t));
            ctx.completeNow();
        }));
    }

    /**
     * Verifies that the CoAP adapter rejects messages from a gateway for a device that it is not authorized
     * for with a 403.
     *
     * @param endpointConfig The endpoints to use for uploading messages.
     * @param ctx The test context
     */
    @ParameterizedTest(name = IntegrationTestSupport.PARAMETERIZED_TEST_NAME_PATTERN)
    @MethodSource("authenticatedGatewayTelemetryVariants")
    @Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
    public void testUploadMessageFailsForUnauthorizedGateway(
            final CoapCommandEndpointConfiguration endpointConfig,
            final VertxTestContext ctx) {

        // GIVEN a device that is connected via gateway "not-the-created-gateway"
        final Tenant tenant = new Tenant();
        final String gatewayId = helper.getRandomDeviceId(tenantId);
        final Device deviceData = new Device().setVia(List.of("not-the-created-gateway"));

        createConsumer(tenantId, msg -> {
            ctx.failNow(new IllegalStateException("consumer should not have received message"));
        })
        .compose(consumer -> helper.registry.addPskDeviceForTenant(tenantId, tenant, gatewayId, SECRET))
        .compose(ok -> helper.registry.registerDevice(tenantId, deviceId, deviceData))
        .compose(ok -> {

            // WHEN another gateway tries to upload a message for the device
            final CoapDevice gw = new CoapDevice(tenantId, gatewayId, SECRET, endpointConfig);
            final OptionSet options = new OptionSet().setContentFormat(MediaTypeRegistry.TEXT_PLAIN);
            return gw.uploadData(deviceId, options, Buffer.buffer("hello"));
        })
        .setHandler(ctx.failing(t -> {
            // THEN the request fails
            ctx.verify(() -> assertServiceInvocationErrorCode(HttpURLConnection.HTTP_FORBIDDEN, t));
            ctx.completeNow();
        }));
    }

    /**
     * Verifies that the CoAP adapter delivers a command to a device and accepts
     * the corresponding response from the device.
     * 
     * @param endpointConfig The endpoints to use for sending/receiving commands.
     * @param ctx The test context.
     * @throws InterruptedException if the test fails.
     */
    @ParameterizedTest(name = IntegrationTestSupport.PARAMETERIZED_TEST_NAME_PATTERN)
    @MethodSource("commandAndControlVariants")
    public void testUploadMessagesWithTtdThatReplyWithCommand(
            final CoapCommandEndpointConfiguration endpointConfig,
            final VertxTestContext ctx) throws InterruptedException {

        final Tenant tenant = new Tenant();
        testUploadMessagesWithTtdThatReplyWithCommand(endpointConfig, tenant, ctx);
    }

    private void testUploadMessagesWithTtdThatReplyWithCommand(
            final CoapCommandEndpointConfiguration endpointConfig,
            final Tenant tenant, final VertxTestContext ctx) throws InterruptedException {

        final String expectedCommand = String.format("%s=%s", Constants.HEADER_COMMAND, COMMAND_TO_SEND);
        final Buffer commandResponseBody = Buffer.buffer("ok");

        final VertxTestContext setup = new VertxTestContext();

        helper.registry.addPskDeviceForTenant(tenantId, tenant, deviceId, SECRET).setHandler(setup.completing());
        ctx.verify(() -> assertThat(setup.awaitCompletion(5, TimeUnit.SECONDS)).isTrue());

        final ClientDevice<OptionSet, OptionSet> client = new CoapDevice(tenantId, deviceId, SECRET, endpointConfig);

        final String commandTargetDeviceId = endpointConfig.isGatewayClient()
                ? helper.setupGatewayDeviceBlocking(tenantId, deviceId, 5)
                : deviceId;
        final String subscribingDeviceId = endpointConfig.isGatewayClientForSingleDevice() ? commandTargetDeviceId
                : deviceId;

        final AtomicInteger counter = new AtomicInteger();

        testUploadMessages(ctx, tenantId,
                () -> warmUp(client, commandTargetDeviceId),
                msg -> {

                    TimeUntilDisconnectNotification.fromMessage(msg)
                        .map(notification -> {
                            logger.trace("received piggy backed message [ttd: {}]: {}", notification.getTtd(), msg);
                            ctx.verify(() -> {
                                assertThat(notification.getTenantId())
                                    .as("notification must contain client's tenant ID")
                                    .isEqualTo(tenantId);
                                assertThat(notification.getDeviceId())
                                    .as("notification must contain client's device ID")
                                    .isEqualTo(subscribingDeviceId);
                            });
                            // now ready to send a command
                            final JsonObject inputData = new JsonObject().put(COMMAND_JSON_KEY, (int) (Math.random() * 100));
                            return helper.sendCommand(
                                    tenantId,
                                    commandTargetDeviceId,
                                    COMMAND_TO_SEND,
                                    "application/json",
                                    inputData.toBuffer(),
                                    // set "forceCommandRerouting" message property so that half the command are rerouted via the AMQP network
                                    IntegrationTestSupport.newCommandMessageProperties(() -> counter.getAndIncrement() >= MESSAGES_TO_SEND / 2),
                                    notification.getMillisecondsUntilExpiry())
                                    .map(response -> {
                                        ctx.verify(() -> {
                                            assertThat(response.getContentType()).isEqualTo("text/plain");
                                            assertThat(response.getApplicationProperty(MessageHelper.APP_PROPERTY_DEVICE_ID, String.class))
                                                .as("command response must contain command target device's identifier")
                                                .isEqualTo(commandTargetDeviceId);
                                            assertThat(response.getApplicationProperty(MessageHelper.APP_PROPERTY_TENANT_ID, String.class))
                                                .as("command response must contain command target device's tenant ID")
                                                .isEqualTo(tenantId);
                                        });
                                        return response;
                                    });
                        });
                },
                count -> {
                    return client.sendEmptyNotification(commandTargetDeviceId, 4)
                            .map(responseOptions -> {
                                ctx.verify(() -> {
                                    assertResponseContainsCommand(
                                            endpointConfig,
                                            responseOptions,
                                            expectedCommand,
                                            tenantId,
                                            commandTargetDeviceId);
                                });
                                return responseOptions.getLocationPathString();
                            })
                            .compose(locationPath -> {
                                // send a response to the command now
                                return client.sendCommandResponse(locationPath, HttpURLConnection.HTTP_OK, "text/plain", commandResponseBody)
                                        .recover(thr -> {
                                            // wrap exception, making clear it occurred when sending the command response,
                                            // not the preceding telemetry/event message
                                            final String msg = "Error sending command response: " + thr.getMessage();
                                            return Future.failedFuture(thr instanceof ServiceInvocationException
                                                    ? new ServiceInvocationException(((ServiceInvocationException) thr).getErrorCode(), msg, thr)
                                                    : new RuntimeException(msg, thr));
                                        });
                            });
                });
    }

    private void assertResponseContainsCommand(
            final CoapCommandEndpointConfiguration endpointConfiguration,
            final OptionSet responseOptions,
            final String expectedCommand,
            final String tenantId,
            final String commandTargetDeviceId) {

        assertThat(responseOptions.getLocationQuery())
            .as("response must contain command")
            .contains(expectedCommand);
        assertThat(responseOptions.getContentFormat()).isEqualTo(MediaTypeRegistry.APPLICATION_JSON);
        int idx = 0;
        assertThat(responseOptions.getLocationPath()).contains(CommandConstants.COMMAND_RESPONSE_ENDPOINT, Index.atIndex(idx++));
        if (endpointConfiguration.isGatewayClient()) {
            assertThat(responseOptions.getLocationPath())
                .as("location path [%s] must contain command target device's tenant ID at index [%d]", responseOptions.getLocationPathString(), idx)
                .contains(tenantId, Index.atIndex(idx++));
            assertThat(responseOptions.getLocationPath())
                .as("location path [%s] must contain command target device ID at index %d", responseOptions.getLocationPathString(), idx)
                .contains(commandTargetDeviceId, Index.atIndex(idx++));
        }
        // request ID
        assertThat(responseOptions.getLocationPath().get(idx))
            .as("location path must contain command request ID")
            .isNotNull();
    }

    /**
     * Verifies that the CoAP adapter delivers a one-way command to a device.
     *
     * @param endpointConfig The endpoints to use for sending/receiving commands.
     * @param ctx The test context
     * @throws InterruptedException if the test fails.
     */
    @ParameterizedTest(name = IntegrationTestSupport.PARAMETERIZED_TEST_NAME_PATTERN)
    @MethodSource("commandAndControlVariants")
    @Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
    public void testUploadMessagesWithTtdThatReplyWithOneWayCommand(
            final CoapCommandEndpointConfiguration endpointConfig,
            final VertxTestContext ctx) throws InterruptedException {

        final Tenant tenant = new Tenant();
        final String expectedCommand = String.format("%s=%s", Constants.HEADER_COMMAND, COMMAND_TO_SEND);

        final VertxTestContext setup = new VertxTestContext();
        helper.registry.addPskDeviceForTenant(tenantId, tenant, deviceId, SECRET).setHandler(setup.completing());
        ctx.verify(() -> assertThat(setup.awaitCompletion(5, TimeUnit.SECONDS)).isTrue());

        final CoapDevice client = new CoapDevice(tenantId, deviceId, SECRET, endpointConfig);
        final AtomicInteger counter = new AtomicInteger();

        // commandTargetDeviceId contains the identifier of the device that the message being
        // uploaded originated from
        final String commandTargetDeviceId = endpointConfig.isGatewayClient()
                ? helper.setupGatewayDeviceBlocking(tenantId, deviceId, 5)
                : deviceId;
        // subscribingDeviceId contains the identifier of the device/gateway that has sent the request message
        final String subscribingDeviceId = endpointConfig.isGatewayClientForSingleDevice() ? commandTargetDeviceId
                : deviceId;

        testUploadMessages(ctx, tenantId,
                () -> warmUp(client, commandTargetDeviceId),
                msg -> {
                    final Integer ttd = MessageHelper.getTimeUntilDisconnect(msg);
                    logger.debug("received downstream message [ttd: {}]: {}", ttd, msg);
                    TimeUntilDisconnectNotification.fromMessage(msg).ifPresent(notification -> {
                        ctx.verify(() -> {
                            assertThat(notification.getTenantId())
                                .as("notification must contain client's tenant ID")
                                .isEqualTo(tenantId);
                            assertThat(notification.getDeviceId())
                                .as("notification must contain client's device ID")
                                .isEqualTo(subscribingDeviceId);
                        });
                        final JsonObject inputData = new JsonObject().put(COMMAND_JSON_KEY, (int) (Math.random() * 100));
                        helper.sendOneWayCommand(
                                tenantId,
                                commandTargetDeviceId,
                                COMMAND_TO_SEND,
                                "application/json",
                                inputData.toBuffer(),
                                // set "forceCommandRerouting" message property so that half the command are rerouted via the AMQP network
                                IntegrationTestSupport.newCommandMessageProperties(() -> counter.getAndIncrement() >= MESSAGES_TO_SEND / 2),
                                notification.getMillisecondsUntilExpiry() / 2);
                    });
                },
                count -> {
                    return client.sendEmptyNotification(commandTargetDeviceId, 4)
                            .map(responseOptions -> {
                                ctx.verify(() -> {
                                    assertResponseContainsOneWayCommand(
                                            endpointConfig,
                                            responseOptions,
                                            expectedCommand,
                                            tenantId,
                                            commandTargetDeviceId);
                                });
                                return responseOptions;
                            });
                });
    }

    private void assertResponseContainsOneWayCommand(
            final CoapCommandEndpointConfiguration endpointConfiguration,
            final OptionSet responseOptions,
            final String expectedCommand,
            final String tenantId,
            final String commandTargetDeviceId) {

        assertThat(responseOptions.getLocationQuery())
            .as("location path must contain command")
            .contains(expectedCommand);
        assertThat(responseOptions.getContentFormat()).isEqualTo(MediaTypeRegistry.APPLICATION_JSON);
        assertThat(responseOptions.getLocationPath()).contains(CommandConstants.COMMAND_ENDPOINT, Index.atIndex(0));
        if (endpointConfiguration.isGatewayClient()) {
            assertThat(responseOptions.getLocationPath())
                .as("location path [%s] must contain command target device's tenant ID", responseOptions.getLocationPathString())
                .contains(tenantId, Index.atIndex(1));
            assertThat(responseOptions.getLocationPath())
                .as("location path [%s] must contain command target device ID", responseOptions.getLocationPathString())
                .contains(commandTargetDeviceId, Index.atIndex(2));
        }
    }

    private void assertMessageProperties(final VertxTestContext ctx, final Message msg) {

        ctx.verify(() -> {
            assertThat(MessageHelper.getDeviceId(msg)).isNotNull();
            assertThat(MessageHelper.getTenantIdAnnotation(msg)).isNotNull();
            assertThat(MessageHelper.getDeviceIdAnnotation(msg)).isNotNull();
            assertThat(MessageHelper.getRegistrationAssertion(msg)).isNull();
            assertThat(msg.getCreationTime()).isGreaterThan(0);
        });
        assertAdditionalMessageProperties(ctx, msg);
    }

    /**
     * Performs additional checks on messages received by a downstream consumer.
     * <p>
     * This default implementation does nothing. Subclasses should override this method to implement
     * reasonable checks.
     * 
     * @param ctx The test context.
     * @param msg The message to perform checks on.
     */
    protected void assertAdditionalMessageProperties(final VertxTestContext ctx, final Message msg) {
        // empty
    }

    /**
     * Performs additional checks on the response received in reply to a CoAP request.
     * <p>
     * This default implementation always returns a succeeded future.
     * Subclasses should override this method to implement reasonable checks.
     * 
     * @param responseOptions The CoAP options from the response.
     * @return A future indicating the outcome of the checks.
     */
    protected Future<?> assertCoapResponse(final OptionSet responseOptions) {
        return Future.succeededFuture();
    }
}
