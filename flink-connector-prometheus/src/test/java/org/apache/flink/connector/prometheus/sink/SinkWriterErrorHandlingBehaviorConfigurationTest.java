/* //////////////////////////////////////////////////////////////////////////////////////////////////////
/
/
/       Project         :   Apache Flink Prometheus Sink Connector
/
/       File            :   SinkWriterErrorHandlingBehaviorConfigurationTest.java
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

/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.flink.connector.prometheus.sink;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SinkWriterErrorHandlingBehaviorConfigurationTest {

    private static final PrometheusSinkConfiguration.SinkWriterErrorHandlingBehaviorConfiguration
            DEFAULT_CONFIG =
                    PrometheusSinkConfiguration.SinkWriterErrorHandlingBehaviorConfiguration
                            .builder()
                            .build();

    @Test
    public void shouldDefaultToFailOnMaxRetryExceeded() {
        assertEquals(
                PrometheusSinkConfiguration.OnErrorBehavior.FAIL,
                DEFAULT_CONFIG.getOnMaxRetryExceeded());
    }

    @Test
    public void shouldDefaultToDiscardAndContinueOnPrometheusNonRetryableError() {
        assertEquals(
                PrometheusSinkConfiguration.OnErrorBehavior.DISCARD_AND_CONTINUE,
                DEFAULT_CONFIG.getOnPrometheusNonRetryableError());
    }

    @Test
    public void shouldPreventSettingContinueOnPrometheusNonRetryableErrorToFail() {
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        PrometheusSinkConfiguration.SinkWriterErrorHandlingBehaviorConfiguration
                                .builder()
                                .onPrometheusNonRetryableError(
                                        PrometheusSinkConfiguration.OnErrorBehavior.FAIL)
                                .build());
    }
}
