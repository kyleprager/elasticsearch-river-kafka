Kafka River Plugin for ElasticSearch
==================================

The Kafka River plugin allows index bulk format messages into elasticsearch.


1. Build this plugin:

        mvn compile test package 
        # this will create a file here: target/releases/elasticsearch-river-kafka-1.0.1-SNAPSHOT.zip
        PLUGIN_PATH=`pwd`/target/releases/elasticsearch-river-kafka-1.0.1-SNAPSHOT.zip

2. Install the PLUGIN

        cd $ELASTICSEARCH_HOME
        ./bin/plugin -url file://$PLUGIN_PATH -install elasticsearch-river-kafka

3. Updating the plugin

        cd $ELASTICSEARCH_HOME
        ./bin/plugin -remove elasticsearch-river-kafka
        ./bin/plugin -url file://$PLUGIN_PATH -install elasticsearch-river-kafka

##### Version Support

ElasticSearch version 1.0.1


Deployment
==========

Notable config values:

bulk_timeout_millis - amount of time before Elasticsearch bulk object is flushed if it is not full yet
bulk_size_messages  - number of messages to send in bulk to Elasticsearch
auto.commit.interval.ms - number of millis before offset is written back to Zookeeper
topic      - Kafka topic
partitions - array of partions as ints.  Each partition is read from in a separate thread.
group_id   - Kafka groupId to register with.  This will affect which offset to pickup when starting and write to Zookeeper every auto.commit.interval.ms millis

Creating the Kafka river is as simple as (all configuration parameters are provided, with default values):

    curl -XPUT 'localhost:9200/_river/es-river-kafka/_meta' -d '{
    	"type" : "kafka",
    	"kafka" : {
        	"broker_host" : "localhost", 
        	"zookeeper" : "localhost",
        	"zookeeper.session.timeout.ms" : "400",
        	"zookeeper.sync.time.ms" : "200",
        	"auto.commit.interval.ms" : "1000",
        	"topic" : "test",
        	"partitions" : [0],
        	"group_id": "es-river-kafka-group-id",
        	"broker_port" : 9092
    	},
    	"index" : {
	        "index_name": "kafka-index",
	        "type_name": "kafka-type",
	        "bulk_timeout_millis": 500, 
	        "bulk_size_messages": 5000
	    }
	}'

Contributors
-------------
 - [Kyle Prager](https://github.com/kyleprager)
 - [Jason Trost](https://github.com/jt6211/)
 - [Mark Conlin](https://github.com/meconlin)


