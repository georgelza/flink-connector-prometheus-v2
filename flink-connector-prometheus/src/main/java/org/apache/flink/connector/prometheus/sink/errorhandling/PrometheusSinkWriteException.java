/* //////////////////////////////////////////////////////////////////////////////////////////////////////
/
/
/       Project         :   Apache Flink Prometheus Sink Connector
/
/       File            :   PrometheusSinkWriteException.java
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

package org.apache.flink.connector.prometheus.sink.errorhandling;

import org.apache.flink.annotation.Internal;

/** Exception during writing to Prometheus Remote-Write endpoint. */
@Internal
public class PrometheusSinkWriteException extends RuntimeException {

    public PrometheusSinkWriteException(String reason) {
        super("Reason: " + reason);
    }

    public PrometheusSinkWriteException(String reason, Exception cause) {
        super("Reason: " + reason, cause);
    }

    public PrometheusSinkWriteException(
            String reason,
            int httpStatusCode,
            String httpReasonPhrase,
            int timeSeriesCount,
            long sampleCount,
            String httpResponseBody) {
        super(
                String.format(
                        "Reason: %s. Http response: %d,%s (%s) .The offending write-request contains %d time-series and %d samples",
                        reason,
                        httpStatusCode,
                        httpReasonPhrase,
                        httpResponseBody,
                        timeSeriesCount,
                        sampleCount));
    }
}
