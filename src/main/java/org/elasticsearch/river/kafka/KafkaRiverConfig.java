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

import java.util.ArrayList;
import java.util.Map;

import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.river.RiverSettings;

public class KafkaRiverConfig {
	
	public final String zookeeper;
	public final String brokerHost;
	public final int brokerPort;
	public final String topic;
	public final String groupId;
	public final ArrayList<Integer> partitions;
	public final Integer zookeeperSessionTimeoutMs;
	public final Integer zookeeperSyncTimeMs;
	public final Integer autoCommitIntervalMs;
	
	public final String indexName;
	public final String typeName;
	public final int bulkSizeMessages;
	public final long bulkTimeoutMillis;
    
    @SuppressWarnings("unchecked")
    public KafkaRiverConfig(RiverSettings settings)
    {
    	if (settings.settings().containsKey("kafka")) {
            Map<String, Object> kafkaSettings = (Map<String, Object>) settings.settings().get("kafka");
            
            topic = (String)kafkaSettings.get("topic");
            zookeeper = XContentMapValues.nodeStringValue(kafkaSettings.get("zookeeper"), "localhost");
            brokerHost = XContentMapValues.nodeStringValue(kafkaSettings.get("broker_host"), "localhost");
            groupId = XContentMapValues.nodeStringValue(kafkaSettings.get("group_id"), "localhost");
            brokerPort = XContentMapValues.nodeIntegerValue(kafkaSettings.get("broker_port"), 9092);
            zookeeperSessionTimeoutMs = XContentMapValues.nodeIntegerValue(kafkaSettings.get("zookeeper.session.timeout.ms"), 400);
            zookeeperSyncTimeMs = XContentMapValues.nodeIntegerValue(kafkaSettings.get("zookeeper.sync.time.ms"), 200);
            autoCommitIntervalMs = XContentMapValues.nodeIntegerValue(kafkaSettings.get("auto.commit.interval.ms"), 1000);
            if (XContentMapValues.isArray(kafkaSettings.get("partitions"))) {
                
                partitions = (ArrayList<Integer>) kafkaSettings.get("partitions");
            } else {
                partitions = new ArrayList<Integer>(1);
                partitions.add(0);
            }
        }
    	else
    	{
    		zookeeper = "localhost";
    		brokerHost = "localhost";
    		brokerPort = 9092;
    		topic = "default_topic";
    		partitions = new ArrayList<Integer>(1);
            partitions.add(0);
    		zookeeperSessionTimeoutMs = 400;
            zookeeperSyncTimeMs = 200;
            autoCommitIntervalMs = 1000;
            groupId = "ES_RIVER_KAFKA_GROUP_ID";
    	}
        
        if (settings.settings().containsKey("index")) {
            Map<String, Object> indexSettings = (Map<String, Object>) settings.settings().get("index");
            bulkSizeMessages = XContentMapValues.nodeIntegerValue(indexSettings.get("bulk_size_messages"), 5000);
            bulkTimeoutMillis = XContentMapValues.nodeIntegerValue(indexSettings.get("bulk_timeout_millis"), 500);
            indexName = XContentMapValues.nodeStringValue(indexSettings.get("index_name"), "kafka-index");
            typeName = XContentMapValues.nodeStringValue(indexSettings.get("type_name"), "kafka-type");
        } else {
            bulkSizeMessages = 5000;
            bulkTimeoutMillis = 500;
            indexName = "kafka-index";
            typeName = "kafka-type";
        }
    }
}
