package com.grepfa;

import software.amazon.awssdk.crt.CRT;
import software.amazon.awssdk.crt.mqtt.MqttClientConnection;
import software.amazon.awssdk.crt.mqtt.MqttClientConnectionEvents;
import software.amazon.awssdk.crt.mqtt.MqttMessage;
import software.amazon.awssdk.crt.mqtt.QualityOfService;
import software.amazon.awssdk.iot.AwsIotMqttConnectionBuilder;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

public class Main {

    // 인증서 파일 경로
    private static final String certPath = "C:\\Users\\vl0011\\Desktop\\grepfa\\iotDeviceSample\\src\\main\\resources\\cert\\sample_device.cert.pem";

    // 키 파일 경로
    private static final String keyPath = "C:\\Users\\vl0011\\Desktop\\grepfa\\iotDeviceSample\\src\\main\\resources\\cert\\sample_device.private.key";

    // 디바이스 클라이언트 ID
    private static final String clientId = "sdk-java";

    // 서버 주소
    private static final String endpoint = "a2bp9adt6od3cn-ats.iot.ap-northeast-2.amazonaws.com";

    // server -> client subscribe topic
    private static final String stocTopic = "sdk/test/java/s";

    // client -> server publish topic
    private static final String ctosTopic = "sdk/test/java";


    private static final int inputCount = 10;

    private static final String message = "hello";


    public static void main(String[] args) {

        System.out.println("Program start!");

        // 1. Define iot event
        MqttClientConnectionEvents event = new MqttClientConnectionEvents() {

            // connection interrupt event
            @Override
            public void onConnectionInterrupted(int errorCode) {
                if (errorCode != 0) {
                    System.out.println("Connection interrupted: " + errorCode + ": " + CRT.awsErrorString(errorCode));
                }
            }

            // connection resume event
            @Override
            public void onConnectionResumed(boolean sessionPresent) {
                System.out.println("Connection resumed: " + (sessionPresent ? "existing session" : "clean session"));
            }
        };

        // 2. Create Mqtt connection

        MqttClientConnection connection = null;

        // Create new iot connection with tls cert file.
        try(AwsIotMqttConnectionBuilder builder = AwsIotMqttConnectionBuilder.newMtlsBuilderFromPath(certPath, keyPath);) {

            // builder config

            // 2 - 1. set connection callback
            builder
                    // set callback
                    .withConnectionEventCallbacks(event)

                    // server endpoint
                    .withEndpoint(endpoint)

                    // client id
                    .withClientId(clientId)

                    // time out config
                    .withProtocolOperationTimeoutMs(60000)

                    // clean up session config
                    .withCleanSession(true);

            connection = builder.build();
        } catch (Exception e) {
            System.out.println("connection build error: " + e.getMessage());
            System.exit(1);
        }


        // 3. Connect the MQTT client
        CompletableFuture<Boolean> connected = connection.connect();
        try {
            boolean sessionPresent = connected.get();
            System.out.println("Connected to " + (!sessionPresent ? "new" : "existing") + " session!");
        } catch (Exception e) {
            System.out.println("connection error: " + e.getMessage());
            System.exit(1);
        }

        CountDownLatch countDownLatch = new CountDownLatch(inputCount);

        // Subscribe to the topic

//        CompletableFuture<Integer> subscribed = connection.subscribe(stocTopic, QualityOfService.AT_LEAST_ONCE, (message) -> {
//            String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
//            System.out.println("MESSAGE: " + payload);
//            countDownLatch.countDown();
//        });
//        try {
//            subscribed.get();
//        } catch (InterruptedException | ExecutionException e) {
//            System.out.println("subscribe error: " + e.getMessage());
//            System.exit(1);
//        }

        // publish topic
        try {
            int count = 0;
            while (count++ < inputCount) {
                CompletableFuture<Integer> published = connection.publish(new MqttMessage(ctosTopic, message.getBytes(), QualityOfService.AT_LEAST_ONCE, false));
                published.get();
                Thread.sleep(1000);
            }
            countDownLatch.await();
        } catch (Exception e) {
            System.out.println("publish error: " + e.getMessage());
            System.exit(1);
        }

        try {
            // Disconnect
            CompletableFuture<Void> disconnected = connection.disconnect();
            disconnected.get();

            // Close the connection now that we are completely done with it.
            connection.close();
        } catch (Exception e) {
            System.out.println("disconnect error: " + e.getMessage());
            System.exit(1);
        }

    }
}