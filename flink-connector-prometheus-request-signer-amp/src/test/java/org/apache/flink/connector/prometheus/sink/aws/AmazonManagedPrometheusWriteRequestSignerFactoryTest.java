/* //////////////////////////////////////////////////////////////////////////////////////////////////////
/
/
/       Project         :   Apache Flink Prometheus Sink Connector
/
/       File            :   AmazonManagedPrometheusWriteRequestSignerFactoryTest.java
/
/       Description     :   Apache Flink Prometheus Sink Connector
/
/       Created     	:   June 2025
/
/       Modified       	:   November 2026
/
/       copyright       :   Copyright 2026, - G Leonard, georgelza@gmail.com
/
/       GIT Repo        :   https://github.com/georgelza/flink-connector-prometheus.git
/
/		ORIGNAL	Source	:	https://github.com/apache/flink-connector-prometheus.git
/
*///////////////////////////////////////////////////////////////////////////////////////////////////////

package org.apache.flink.connector.prometheus.sink.aws;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.prometheus.sink.PrometheusRequestSigner;
import org.apache.flink.connector.prometheus.table.PrometheusConfig;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_REGION;
import static org.apache.flink.connector.prometheus.table.PrometheusConnectorOption.METRIC_REMOTE_WRITE_URL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AmazonManagedPrometheusWriteRequestSignerFactoryTest {
    private AmazonManagedPrometheusWriteRequestSignerFactory factory;

    @BeforeEach
    public void setup() {
        factory = new AmazonManagedPrometheusWriteRequestSignerFactory();
    }

    @Test
    void testIdentifier() {
        assertEquals(factory.requestSignerIdentifer(), "amazon-managed-prometheus");
    }

    @Test
    void testCreateRequestSigner() {
        final String endpoint =
                "https://aps-workspaces.us-east-1.amazonaws.com/workspaces/abc/api/v1/remote_write";
        final String region = "us-east-1";
        Configuration config = new Configuration();
        config.set(METRIC_REMOTE_WRITE_URL, endpoint);
        config.setString(AWS_REGION, region);

        PrometheusRequestSigner requestSigner =
                factory.getRequestSigner(new PrometheusConfig(config));

        assertInstanceOf(AmazonManagedPrometheusWriteRequestSigner.class, requestSigner);
    }

    @Test
    void testCreateRequestSignerFailsWithInvalidRegion() {
        final String endpoint =
                "https://aps-workspaces.us-east-1.amazonaws.com/workspaces/abc/api/v1/remote_write";
        final String region = "invalid-region";
        Configuration config = new Configuration();
        config.set(METRIC_REMOTE_WRITE_URL, endpoint);
        config.setString(AWS_REGION, region);

        assertThrows(
                IllegalArgumentException.class,
                () -> factory.getRequestSigner(new PrometheusConfig(config)));
    }

    @Test
    void testCreateRequestSignerFailsWithInvalidURL() {
        Configuration config = new Configuration();
        config.set(METRIC_REMOTE_WRITE_URL, "invalid-endpoint");
        config.setString(AWS_REGION, "us-east-1");

        assertThrows(
                IllegalArgumentException.class,
                () -> factory.getRequestSigner(new PrometheusConfig(config)));
    }
}
