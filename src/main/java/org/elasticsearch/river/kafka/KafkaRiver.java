/* Copyright 2013 Endgame, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.river.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;

/**
 * KafkaRiver
 * 
 */
public class KafkaRiver extends AbstractRiverComponent implements River {

    private final Client client;
    private final KafkaRiverConfig riverConfig;

    private final int numThreads; // one thread per partition

    private volatile Thread[] threads;
    private volatile Thread flusherThread;

    private ConsumerConnector consumer;

    @Inject
    public KafkaRiver(RiverName riverName, RiverSettings settings, Client client) {
        super(riverName, settings);

        this.client = client;

        try {
            logger.info("KafkaRiver created: name={}, type={}", riverName.getName(), riverName.getType());
            this.riverConfig = new KafkaRiverConfig(settings);
            numThreads = riverConfig.partitions.size(); // one thread per partition
        } catch (Exception e) {
            logger.error("Unexpected Error occurred", e);
            throw new RuntimeException(e);
        }

    }

    @Override
    public void start() {
        try {
            logger.info("creating kafka river: zookeeper = {}, broker = {}, broker_port = {}", riverConfig.zookeeper,
                    riverConfig.brokerHost, riverConfig.brokerPort);
            logger.info("part = {}, topic = {}, groupId = {}", riverConfig.partitions, riverConfig.topic);
            logger.info("bulkSize = {}, bulkTimeout = {}", riverConfig.bulkSizeMessages, riverConfig.bulkTimeoutMillis);
            logger.info("zktimeout = {}, zksync = {}, autocommit = {}",riverConfig.zookeeperSessionTimeoutMs.toString(),
                    riverConfig.zookeeperSyncTimeMs.toString(), riverConfig.autoCommitIntervalMs.toString());
            logger.info("Partitions: " + riverConfig.partitions);

            // create array of Thread objects to save off so we can shut the down on close()
            threads = new Thread[numThreads];

            // this is just another version of the same thing
            KafkaStreamReader[] kafkaThreads = new KafkaStreamReader[numThreads];

            // create High Level Consumer
            consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig(riverConfig.zookeeper,
                    riverConfig.groupId, riverConfig.zookeeperSessionTimeoutMs.toString(),
                    riverConfig.zookeeperSyncTimeMs.toString(), riverConfig.autoCommitIntervalMs.toString()));
            Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
            topicCountMap.put(riverConfig.topic, new Integer(numThreads)); // number of threads
            Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
            List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(riverConfig.topic);
            logger.info("Number of streams: " + streams.size());

            // create one thread per Kafka stream
            int threadNumber = 0;
            for (final KafkaStream<byte[], byte[]> stream : streams) {
                KafkaStreamReader thread = new KafkaStreamReader(stream, client, threadNumber, riverConfig.indexName, riverConfig.typeName, riverConfig.bulkSizeMessages);
                kafkaThreads[threadNumber] = thread;
                Thread esthread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "kafka_river").newThread(
                        thread);
                esthread.start();
                threads[threadNumber] = esthread;
                threadNumber++;
            }

            // add time based flushing
            Runnable flusher = new TimeBasedFlusher(kafkaThreads, riverConfig.bulkTimeoutMillis);
            flusherThread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "kafka_river")
                    .newThread(flusher);
            flusherThread.start();

        } catch (Exception e) {
            logger.error("Unexpected Error occurred", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        consumer.shutdown(); // shutdown high level consumer
        try {
            logger.info("closing kafka river");
            for (int i = 0; i < threads.length; i++) {
                threads[i].interrupt();
            }
            flusherThread.interrupt();
        } catch (Exception e) {
            logger.error("Unexpected Error occurred", e);
            throw new RuntimeException(e);
        }
    }

    private ConsumerConfig createConsumerConfig(String a_zookeeper, String a_groupId, String zk_session_timeout_ms,
            String zk_sync_time_ms, String auto_commit_interval_ms) {
        Properties props = new Properties();
        props.put("zookeeper.connect", a_zookeeper);
        props.put("group.id", a_groupId);
        props.put("zookeeper.session.timeout.ms", zk_session_timeout_ms);
        props.put("zookeeper.sync.time.ms", zk_sync_time_ms);
        props.put("auto.commit.interval.ms", auto_commit_interval_ms);

        return new ConsumerConfig(props);
    }

    /**
     * START PRIVATE CLASSES
     * 
     * @author kyleprager
     * 
     */

    /**
     * Flushes out each Kafka stream after a certain time period
     * 
     * @author kyleprager
     * 
     */
    private class TimeBasedFlusher implements Runnable {
        private final long TIMEOUT; // timeout in ms
        private KafkaStreamReader[] streamReaders;

        /**
         * 
         * @param readers
         *            - Array of KafkaStreamReader threads we will flush out after our TIMEOUT
         */
        public TimeBasedFlusher(KafkaStreamReader[] readers, long timeout) {
            streamReaders = readers;
            this.TIMEOUT = timeout;
        }

        /**
         * Loop through Kafka streams, sleep for TIMEOUT millis, then flush them into ES.
         */
        @Override
        public void run() {
            while (true) {
                // sleep
                try {
                    Thread.sleep(TIMEOUT);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    // flush no matter what
                    flushESBulkData();
                }

                // flush data like normal
                flushESBulkData();
            }
        }

        private void flushESBulkData() {
            // flush
            for (int i = 0; i < streamReaders.length; i++) {
                KafkaStreamReader ksr = streamReaders[i];
                if (System.currentTimeMillis() - ksr.lastBulkRequestTime > TIMEOUT) {
                    ksr.executeBulkRequest();
                }
            }
        }
    }

    private class KafkaStreamReader implements Runnable {
        private KafkaStream<byte[], byte[]> kafkaStream;
        private int threadNumber;
        private Client client;
        private BulkRequestBuilder bulk;
        private int count = 0;
        public long lastBulkRequestTime;

        private final String INDEX;
        private final String TYPE;
        private final int MAX_BULK_SIZE;

        public KafkaStreamReader(KafkaStream<byte[], byte[]> a_stream, Client client, int a_threadNumber, String index, String type, int maxBulkSize) {
            threadNumber = a_threadNumber;
            kafkaStream = a_stream;
            this.client = client;
            this.INDEX = index;
            this.TYPE = type;
            this.MAX_BULK_SIZE = maxBulkSize;
        }

        public void run() {
            ConsumerIterator<byte[], byte[]> it = kafkaStream.iterator();
            bulk = client.prepareBulk();
            int total = 0;

            while (it.hasNext()) {
                String message = new String(it.next().message());
                IndexRequest request = new IndexRequest(INDEX).type(TYPE).source(message);
                bulk.add(request);
                count++;
                total++;
                if (count >= MAX_BULK_SIZE) {
                    executeBulkRequest();
                    lastBulkRequestTime = System.currentTimeMillis();
                }
                logger.debug("Thread " + threadNumber + ": " + message);
            }
            logger.info("Total messages processed: " + total);
            logger.info("Shutting down Thread: " + threadNumber);
        }

        /**
         * process bulk request
         * 
         * @param bulk
         *            - bulk request object that has been loaded with lots of messages
         * @return
         */
        public synchronized void executeBulkRequest() {
            if (count == 0) {
                return;
            }
            BulkResponse response = bulk.execute().actionGet();
            if (response.hasFailures()) {
                logger.error(response.buildFailureMessage());
            } else {
                logger.debug("Just processed " + count + " messages in " + response.getTookInMillis() + " millis into "
                        + INDEX + "/" + TYPE);
            }
            count = 0; // reset count
            bulk = client.prepareBulk(); // reset bulk request builder
        }
    }
}
