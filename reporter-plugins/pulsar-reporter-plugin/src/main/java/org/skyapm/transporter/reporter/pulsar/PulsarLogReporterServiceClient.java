/*
 * Copyright 2021 SkyAPM
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.skyapm.transporter.reporter.pulsar;

import java.util.List;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.skywalking.apm.agent.core.boot.OverrideImplementor;
import org.apache.skywalking.apm.agent.core.boot.ServiceManager;
import org.apache.skywalking.apm.agent.core.logging.api.ILog;
import org.apache.skywalking.apm.agent.core.logging.api.LogManager;
import org.apache.skywalking.apm.agent.core.remote.LogReportServiceClient;
import org.apache.skywalking.apm.agent.core.util.CollectionUtil;
import org.apache.skywalking.apm.network.logging.v3.LogData;

@OverrideImplementor(LogReportServiceClient.class)
public class PulsarLogReporterServiceClient extends LogReportServiceClient implements PulsarConnectionStatusListener {

    private static final ILog LOGGER = LogManager.getLogger(PulsarLogReporterServiceClient.class);
    private String topic;
    private Producer<byte[]> producer;

    @Override
    public void prepare() {
        PulsarProducerManager producerManager = ServiceManager.INSTANCE.findService(PulsarProducerManager.class);
        producerManager.addListener(this);
        topic = producerManager.formatTopicNameThenRegister(PulsarReporterPluginConfig.Plugin.Pulsar.TOPIC_LOGGING);
    }

    @Override
    public void produce(final LogData logData) {
        super.produce(logData);
    }

    @Override
    public void consume(final List<LogData> dataList) {
        if (producer == null || CollectionUtil.isEmpty(dataList)) {
            return;
        }
        try {
            for (LogData data : dataList) {
                producer.newMessage()
                        .key(data.getService())
                        .value(data.toByteArray())
                        .send();
            }
            producer.flush();
        } catch (PulsarClientException e) {
            LOGGER.error(e, "Send log data failed.");
        }
    }

    @Override
    public void onStatusChanged(final PulsarConnectionStatus status) {
        if (status == PulsarConnectionStatus.CONNECTED) {
            producer = ServiceManager.INSTANCE.findService(PulsarProducerManager.class).getProducer(topic);
        }
    }
}