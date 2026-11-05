/* //////////////////////////////////////////////////////////////////////////////////////////////////////
/
/
/       Project         :   Apache Flink Prometheus Sink Connector
/
/       File            :   RethrowingIOSessionListener.java
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
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.prometheus.sink.http;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.prometheus.sink.errorhandling.PrometheusSinkWriteException;

import org.apache.hc.core5.reactor.IOSession;
import org.apache.hc.core5.reactor.IOSessionListener;

/**
 * Selectively rethrow PrometheusSinkWriteException, causing the httpclient to fail. Otherwise, the
 * exception would be swallowed by the IOReactor.
 */
@Internal
public class RethrowingIOSessionListener implements IOSessionListener {
    @Override
    public void exception(IOSession ioSession, Exception e) {
        if (e instanceof PrometheusSinkWriteException) {
            // Rethrow the exception
            throw (PrometheusSinkWriteException) e;
        }
    }

    @Override
    public void connected(IOSession ioSession) {
        // Nothing to do
    }

    @Override
    public void startTls(IOSession ioSession) {
        // Nothing to do
    }

    @Override
    public void inputReady(IOSession ioSession) {
        // Nothing to do
    }

    @Override
    public void outputReady(IOSession ioSession) {
        // Nothing to do
    }

    @Override
    public void timeout(IOSession ioSession) {
        // Nothing to do
    }

    @Override
    public void disconnected(IOSession ioSession) {
        // Nothing to do
    }
}
