/**
 * Copyright (c) 2021 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information regarding copyright ownership.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0
 *
 * SPDX-License-Identifier: EPL-2.0
 */


package org.eclipse.hono.tests.coap;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.eclipse.californium.core.CoapClient;
import org.eclipse.californium.core.coap.CoAP.Code;
import org.eclipse.californium.core.coap.CoAP.ResponseCode;
import org.eclipse.californium.core.coap.OptionSet;
import org.eclipse.californium.core.coap.Request;
import org.eclipse.californium.core.network.config.NetworkConfig;
import org.eclipse.californium.elements.Connector;
import org.eclipse.californium.elements.exception.ConnectorException;
import org.eclipse.californium.scandium.DTLSConnector;
import org.eclipse.californium.scandium.config.DtlsConnectorConfig;
import org.eclipse.californium.scandium.dtls.ClientHandshaker;
import org.eclipse.californium.scandium.dtls.DTLSSession;
import org.eclipse.californium.scandium.dtls.HandshakeException;
import org.eclipse.californium.scandium.dtls.Handshaker;
import org.eclipse.californium.scandium.dtls.ResumingClientHandshaker;
import org.eclipse.californium.scandium.dtls.ResumingServerHandshaker;
import org.eclipse.californium.scandium.dtls.ServerHandshaker;
import org.eclipse.californium.scandium.dtls.SessionAdapter;
import org.eclipse.californium.scandium.dtls.SessionId;
import org.eclipse.californium.scandium.dtls.cipher.CipherSuite;
import org.eclipse.hono.service.management.tenant.Tenant;
import org.eclipse.hono.tests.IntegrationTestSupport;
import org.eclipse.leshan.client.californium.LeshanClient;
import org.eclipse.leshan.client.californium.LeshanClientBuilder;
import org.eclipse.leshan.client.engine.DefaultRegistrationEngineFactory;
import org.eclipse.leshan.client.object.Security;
import org.eclipse.leshan.client.object.Server;
import org.eclipse.leshan.client.resource.LwM2mObjectEnabler;
import org.eclipse.leshan.client.resource.ObjectsInitializer;
import org.eclipse.leshan.client.resource.listener.ObjectsListenerAdapter;
import org.eclipse.leshan.client.servers.ServerIdentity;
import org.eclipse.leshan.core.LwM2mId;
import org.eclipse.leshan.core.californium.DefaultEndpointFactory;
import org.eclipse.leshan.core.model.LwM2mModel;
import org.eclipse.leshan.core.model.ObjectLoader;
import org.eclipse.leshan.core.model.ObjectModel;
import org.eclipse.leshan.core.model.StaticModel;
import org.eclipse.leshan.core.node.codec.DefaultLwM2mNodeDecoder;
import org.eclipse.leshan.core.node.codec.DefaultLwM2mNodeEncoder;
import org.eclipse.leshan.core.request.BindingMode;
import org.eclipse.leshan.core.request.RegisterRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Promise;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

/**
 * Integration tests for sending commands to a device connected to the CoAP adapter.
 *
 */
@ExtendWith(VertxExtension.class)
public class LwM2mIT extends CoapTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(LwM2mIT.class);

    private LeshanClient client;
    private ExampleDevice device;

    @BeforeEach
    void createExampleDevice() {
        device = new ExampleDevice();
    }

    @AfterEach
    void stopClient() {
        if (client != null) {
            client.destroy(true);
        }
    }

    /**
     * Verifies that a registration request to the CoAP adapter's resource directory
     * resource triggers an event with a TTD.
     *
     * @param ctx The vert.x test context.
     * @throws InterruptedException if the fixture cannot be created.
     * @throws ConnectorException if the CoAP adapter is not available.
     * @throws IOException if the CoAP adapter is not available.
     */
    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testRegistrationTriggersEvent(final VertxTestContext ctx) throws InterruptedException, ConnectorException, IOException {

        final Checkpoint notificationReceived = ctx.checkpoint();
        final Checkpoint registrationSucceeded = ctx.checkpoint();
        final int lifetime = 300;
        final int communicationPeriod = lifetime;
        final String endpoint = "test-device";

        helper.registry.addPskDeviceForTenant(tenantId, new Tenant(), deviceId, SECRET)
            .compose(ok -> helper.applicationClient.createEventConsumer(
                    tenantId,
                    msg -> {
                        logger.trace("received event: {}", msg);
                        msg.getTimeUntilDisconnectNotification()
                            .ifPresent(notification -> {
                                ctx.verify(() -> assertThat(notification.getTtd()).isEqualTo(lifetime));
                                notificationReceived.flag();
                            });
                    },
                    remoteClose -> {}))
            .compose(eventConsumer -> {
                final Promise<LeshanClient> result = Promise.promise();
                final var serverURI = getCoapsRequestUri(null);
                try {
                    final var leshanClient = createLeshanClient(
                            endpoint,
                            null,
                            lifetime,
                            communicationPeriod,
                            serverURI.toString(),
                            IntegrationTestSupport.getUsername(deviceId, tenantId).getBytes(StandardCharsets.UTF_8),
                            SECRET.getBytes(StandardCharsets.UTF_8),
                            null,
                            null,
                            null,
                            null,
                            null,
                            false, // do not support deprecated content encoding
                            false, // do not support deprecated ciphers
                            false, // do not perform new DTLS handshake before updating registration
                            false, // try to resume DTLS session
                            null); // use all supported ciphers
                    result.complete(leshanClient);
                } catch (final CertificateEncodingException e) {
                    result.fail(e);
                }
                return result.future();
            })
            .onSuccess(leshanClient -> {
                client = leshanClient;
                client.addObserver(new LeshanClientObserver() {
                    @Override
                    public void onRegistrationSuccess(
                            final ServerIdentity server,
                            final RegisterRequest request,
                            final String registrationID) {
                        registrationSucceeded.flag();
                    }
                });
                client.start();
            });
    }

    /**
     * Verifies that a de-registration request to the CoAP adapter's resource directory
     * resource triggers an event with a TTD.
     *
     * @param ctx The vert.x test context.
     * @throws InterruptedException if the fixture cannot be created.
     * @throws ConnectorException if the CoAP adapter is not available.
     * @throws IOException if the CoAP adapter is not available.
     */
    @Test
    @Disabled
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testDeregistrationTriggersEvent(final VertxTestContext ctx) throws InterruptedException, ConnectorException, IOException {

        final Checkpoint notificationReceived = ctx.checkpoint();

        helper.registry.addPskDeviceForTenant(tenantId, new Tenant(), deviceId, SECRET)
        .compose(ok -> helper.applicationClient.createEventConsumer(
                tenantId,
                msg -> {
                    logger.trace("received event: {}", msg);
                    msg.getTimeUntilDisconnectNotification()
                        .ifPresent(notification -> {
                            ctx.verify(() -> assertThat(notification.getTtd()).isEqualTo(0));
                            notificationReceived.flag();
                        });
                },
                remoteClose -> {}))
        .compose(c -> {
            final CoapClient client = getCoapsClient(deviceId, tenantId, SECRET);
            final Request request = new Request(Code.DELETE);
            request.setURI(getCoapsRequestUri(String.format("/rd/%s:%s", tenantId, deviceId)));
            final Promise<OptionSet> result = Promise.promise();
            client.advanced(getHandler(result, ResponseCode.DELETED), request);
            return result.future();
        })
        .onComplete(ctx.completing());
    }

    private LeshanClient createLeshanClient(
            final String endpoint,
            final Map<String, String> additionalAttributes,
            final int lifetime,
            final Integer communicationPeriod,
            final String serverURI,
            final byte[] pskIdentity,
            final byte[] pskKey,
            final PrivateKey clientPrivateKey,
            final PublicKey clientPublicKey,
            final PublicKey serverPublicKey,
            final X509Certificate clientCertificate,
            final X509Certificate serverCertificate,
            final boolean supportOldFormat,
            final boolean supportDeprecatedCiphers,
            final boolean reconnectOnUpdate,
            final boolean forceFullhandshake,
            final List<CipherSuite> ciphers) throws CertificateEncodingException {

        // Initialize model
        final List<ObjectModel> models = ObjectLoader.loadDefault();

        // Initialize object list
        final LwM2mModel model = new StaticModel(models);
        final ObjectsInitializer initializer = new ObjectsInitializer(model);
        if (pskIdentity != null) {
            initializer.setInstancesForObject(LwM2mId.SECURITY, Security.psk(serverURI, 123, pskIdentity, pskKey));
            initializer.setInstancesForObject(LwM2mId.SERVER, new Server(123, lifetime, BindingMode.U, false));
        } else if (clientPublicKey != null) {
            initializer.setInstancesForObject(LwM2mId.SECURITY, Security.rpk(serverURI, 123, clientPublicKey.getEncoded(),
                    clientPrivateKey.getEncoded(), serverPublicKey.getEncoded()));
            initializer.setInstancesForObject(LwM2mId.SERVER, new Server(123, lifetime, BindingMode.U, false));
        } else if (clientCertificate != null) {
            initializer.setInstancesForObject(LwM2mId.SECURITY, Security.x509(serverURI, 123, clientCertificate.getEncoded(),
                    clientPrivateKey.getEncoded(), serverCertificate.getEncoded()));
            initializer.setInstancesForObject(LwM2mId.SERVER, new Server(123, lifetime, BindingMode.U, false));
        } else {
            initializer.setInstancesForObject(LwM2mId.SECURITY, Security.noSec(serverURI, 123));
            initializer.setInstancesForObject(LwM2mId.SERVER, new Server(123, lifetime, BindingMode.U, false));
        }
        initializer.setInstancesForObject(LwM2mId.DEVICE, device);
        final List<LwM2mObjectEnabler> enablers = initializer.createAll();

        // Create CoAP Config
        final NetworkConfig coapConfig = LeshanClientBuilder.createDefaultNetworkConfig();

        // Create DTLS Config
        final DtlsConnectorConfig.Builder dtlsConfig = new DtlsConnectorConfig.Builder();
        dtlsConfig.setRecommendedCipherSuitesOnly(!supportDeprecatedCiphers);
        if (ciphers != null) {
            dtlsConfig.setSupportedCipherSuites(ciphers);
        }

        // Configure Registration Engine
        final DefaultRegistrationEngineFactory engineFactory = new DefaultRegistrationEngineFactory();
        engineFactory.setCommunicationPeriod(communicationPeriod);
        engineFactory.setReconnectOnUpdate(reconnectOnUpdate);
        engineFactory.setResumeOnConnect(!forceFullhandshake);

        // configure EndpointFactory
        final DefaultEndpointFactory endpointFactory = new DefaultEndpointFactory("LWM2M CLIENT") {
            @Override
            protected Connector createSecuredConnector(final DtlsConnectorConfig dtlsConfig) {

                return new DTLSConnector(dtlsConfig) {
                    @Override
                    protected void onInitializeHandshaker(final Handshaker handshaker) {
                        handshaker.addSessionListener(new SessionAdapter() {

                            private SessionId sessionIdentifier = null;

                            @Override
                            public void handshakeStarted(final Handshaker handshaker) throws HandshakeException {
                                if (handshaker instanceof ResumingServerHandshaker) {
                                    LOG.info("DTLS abbreviated Handshake initiated by server : STARTED ...");
                                } else if (handshaker instanceof ServerHandshaker) {
                                    LOG.info("DTLS Full Handshake initiated by server : STARTED ...");
                                } else if (handshaker instanceof ResumingClientHandshaker) {
                                    sessionIdentifier = handshaker.getSession().getSessionIdentifier();
                                    LOG.info("DTLS abbreviated Handshake initiated by client : STARTED ...");
                                } else if (handshaker instanceof ClientHandshaker) {
                                    LOG.info("DTLS Full Handshake initiated by client : STARTED ...");
                                }
                            }

                            @Override
                            public void sessionEstablished(final Handshaker handshaker, final DTLSSession establishedSession)
                                    throws HandshakeException {
                                if (handshaker instanceof ResumingServerHandshaker) {
                                    LOG.info("DTLS abbreviated Handshake initiated by server : SUCCEED");
                                } else if (handshaker instanceof ServerHandshaker) {
                                    LOG.info("DTLS Full Handshake initiated by server : SUCCEED");
                                } else if (handshaker instanceof ResumingClientHandshaker) {
                                    if (sessionIdentifier != null && sessionIdentifier
                                            .equals(handshaker.getSession().getSessionIdentifier())) {
                                        LOG.info("DTLS abbreviated Handshake initiated by client : SUCCEED");
                                    } else {
                                        LOG.info(
                                                "DTLS abbreviated turns into Full Handshake initiated by client : SUCCEED");
                                    }
                                } else if (handshaker instanceof ClientHandshaker) {
                                    LOG.info("DTLS Full Handshake initiated by client : SUCCEED");
                                }
                            }

                            @Override
                            public void handshakeFailed(final Handshaker handshaker, final Throwable error) {
                                // get cause
                                final String cause;
                                if (error != null) {
                                    if (error.getMessage() != null) {
                                        cause = error.getMessage();
                                    } else {
                                        cause = error.getClass().getName();
                                    }
                                } else {
                                    cause = "unknown cause";
                                }

                                if (handshaker instanceof ResumingServerHandshaker) {
                                    LOG.info("DTLS abbreviated Handshake initiated by server : FAILED ({})", cause);
                                } else if (handshaker instanceof ServerHandshaker) {
                                    LOG.info("DTLS Full Handshake initiated by server : FAILED ({})", cause);
                                } else if (handshaker instanceof ResumingClientHandshaker) {
                                    LOG.info("DTLS abbreviated Handshake initiated by client : FAILED ({})", cause);
                                } else if (handshaker instanceof ClientHandshaker) {
                                    LOG.info("DTLS Full Handshake initiated by client : FAILED ({})", cause);
                                }
                            }
                        });
                    }
                };
            }
        };

        // Create client
        final LeshanClientBuilder builder = new LeshanClientBuilder(endpoint);
        builder.setObjects(enablers);
        builder.setCoapConfig(coapConfig);
        builder.setDtlsConfig(dtlsConfig);
        builder.setRegistrationEngineFactory(engineFactory);
        builder.setEndpointFactory(endpointFactory);
        if (supportOldFormat) {
            builder.setDecoder(new DefaultLwM2mNodeDecoder(true));
            builder.setEncoder(new DefaultLwM2mNodeEncoder(true));
        }
        builder.setAdditionalAttributes(additionalAttributes);
        final LeshanClient client = builder.build();

        client.getObjectTree().addListener(new ObjectsListenerAdapter() {
            @Override
            public void objectRemoved(final LwM2mObjectEnabler object) {
                LOG.info("Object {} disabled.", object.getId());
            }

            @Override
            public void objectAdded(final LwM2mObjectEnabler object) {
                LOG.info("Object {} enabled.", object.getId());
            }
        });

        return client;

//        // Print commands help
//        final StringBuilder commandsHelp = new StringBuilder("Commands available :");
//        commandsHelp.append(System.lineSeparator());
//        commandsHelp.append(System.lineSeparator());
//        commandsHelp.append("  - create <objectId> : to enable a new object.");
//        commandsHelp.append(System.lineSeparator());
//        commandsHelp.append("  - delete <objectId> : to disable a new object.");
//        commandsHelp.append(System.lineSeparator());
//        commandsHelp.append("  - update : to trigger a registration update.");
//        commandsHelp.append(System.lineSeparator());
//        commandsHelp.append("  - w : to move to North.");
//        commandsHelp.append(System.lineSeparator());
//        commandsHelp.append("  - a : to move to East.");
//        commandsHelp.append(System.lineSeparator());
//        commandsHelp.append("  - s : to move to South.");
//        commandsHelp.append(System.lineSeparator());
//        commandsHelp.append("  - d : to move to West.");
//        commandsHelp.append(System.lineSeparator());
//        LOG.info(commandsHelp.toString());
//
//        // Start the client
//        client.start();
//
//        // De-register on shutdown and stop client.
//        Runtime.getRuntime().addShutdownHook(new Thread() {
//            @Override
//            public void run() {
//                client.destroy(true); // send de-registration request before destroy
//            }
//        });
//
//        // Change the location through the Console
//        try (Scanner scanner = new Scanner(System.in)) {
//            List<Character> wasdCommands = Arrays.asList('w', 'a', 's', 'd');
//            while (scanner.hasNext()) {
//                String command = scanner.next();
//                if (command.startsWith("create")) {
//                    try {
//                        int objectId = scanner.nextInt();
//                        if (client.getObjectTree().getObjectEnabler(objectId) != null) {
//                            LOG.info("Object {} already enabled.", objectId);
//                        }
//                        if (model.getObjectModel(objectId) == null) {
//                            LOG.info("Unable to enable Object {} : there no model for this.", objectId);
//                        } else {
//                            ObjectsInitializer objectsInitializer = new ObjectsInitializer(model);
//                            objectsInitializer.setDummyInstancesForObject(objectId);
//                            LwM2mObjectEnabler object = objectsInitializer.create(objectId);
//                            client.getObjectTree().addObjectEnabler(object);
//                        }
//                    } catch (Exception e) {
//                        // skip last token
//                        scanner.next();
//                        LOG.info("Invalid syntax, <objectid> must be an integer : create <objectId>");
//                    }
//                } else if (command.startsWith("delete")) {
//                    try {
//                        int objectId = scanner.nextInt();
//                        if (objectId == 0 || objectId == 0 || objectId == 3) {
//                            LOG.info("Object {} can not be disabled.", objectId);
//                        } else if (client.getObjectTree().getObjectEnabler(objectId) == null) {
//                            LOG.info("Object {} is not enabled.", objectId);
//                        } else {
//                            client.getObjectTree().removeObjectEnabler(objectId);
//                        }
//                    } catch (Exception e) {
//                        // skip last token
//                        scanner.next();
//                        LOG.info("\"Invalid syntax, <objectid> must be an integer : delete <objectId>");
//                    }
//                } else if (command.startsWith("update")) {
//                    client.triggerRegistrationUpdate();
//                } else if (command.length() == 1 && wasdCommands.contains(command.charAt(0))) {
//                    locationInstance.moveLocation(command);
//                } else {
//                    LOG.info("Unknown command '{}'", command);
//                }
//            }
//        }
    }

}
