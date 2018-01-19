[![Build Status](https://travis-ci.org/Landoop/kafka-connect-tools.svg?branch=master)](https://travis-ci.org/Landoop/kafka-connect-tools)
[<img src="https://img.shields.io/badge/latest%20release-v1.0.6-blue.svg?label=latest%20release"/>](http://search.maven.org/#search%7Cga%7C1%7Ca%3A%22kafka-connect-cli%22)

Connect tools is Maven

```bash
<dependency>
	<groupId>com.datamountaineer</groupId>
	<artifactId>kafka-connect-cli</artifactId>
	<version>1.0.6</version>
</dependency>
```

## Requirements

* Java 1.8
* Gradle 3

Kafka Connect CLI
=================

This is a tiny command line interface (CLI) around the [Kafka Connect REST Interface](http://docs.confluent.io/3.0.1/connect/userguide.html#rest-interface) to manage connectors.
It is used in a git like fashion where the first program argument indicates the command: it can be one of `[ps|get|rm|create|run|status|status|plugins|describe|validate|restart|pause|resume]`.

The CLI is meant to behave as a good unix citizen: input from `stdin`; output to `stdout`; 
out of band info to `stderr` and non-zero exit status on error. Commands 
dealing with configuration expect or produce data in .properties 
style: `key=value` lines and comments start with a `#`.

    connect-cli 1.0.6
    Usage: connect-cli [ps|get|rm|create|run|diff|status|plugins|describe|validate|restart|pause|resume] [options] [<connector-name>]

      --help
            prints this usage text
      -e <value> | --endpoint <value>
            Kafka Connect REST URL, default is http://localhost:8083/
      -f <value> | --format <value>
            Format of the config, default is PROPERTIES. Valid options are 'properties' and 'json'.

      Command: ps
      list active connectors names.
      
      Command: get
      get the configuration of the specified connector.
      
      Command: rm
      remove the specified connector.
      
      Command: create
      create the specified connector with the config from stdin; the connector cannot already exist.
      
      Command: run
      create or update the specified connector with the config from stdin.
      
      Command: diff
      diff the specified connector with the config from stdin.
      
      Command: status
      get connector and it's task(s) state(s).
      
      Command: plugins
      list the available connector class plugins on the classpath.
      
      Command: describe
      list the configurations for a connector class plugin on the classpath.
      
      Command: pause
      pause the specified connector.
      
      Command: restart
      restart the specified connector.
      
      Command: resume
      resume the specified connector.
      
      Command: validate
      validate the connector config from stdin against a connector class plugin on the classpath.
      
      Command: task_ps
      list the tasks belonging to a connector.
      
      Command: task_status
      get the status of a connector task.
      
      Command: task_restart
      restart the specified connector task.
    
You can override the default endpoint by setting an environment variable `KAFKA_CONNECT_REST` i.e.

    export KAFKA_CONNECT_REST="http://myserver:myport"

To Build
========

```bash
gradle buildCli
```


Usage
=====

Clone this repository, do a `gradle buildCli` and run the jar in a way you prefer, for example with the provided `cli` shell script. The CLI can be used as follows.

Get Active Connectors
---------------------

Command: `ps`

Example:

    $ bin/connect-cli ps
    twitter-source

Get Connector Configuration
---------------------------

Command: `get`

Example:

    $ bin/connect-cli get twitter-source
    #Connector `twitter-source`:
    name=twitter-source
    tasks.max=1

    (snip)

    track.terms=test
    #task ids: 0

Delete a Connector
------------------

Command: `rm`

Example:

    $ bin/connect-cli rm twitter-source

Create a New Connector
----------------------

The connector cannot already exist.

Command: `create`

Example:

    $ bin/connect-cli create twitter-source <twitter.properties
    #Connector `twitter-source`:
    name=twitter-source
    tasks.max=1

    (snip)

    track.terms=test
    #task ids: 0

Create or Update a Connector
----------------------------

Either starts a new connector if it did not exist, or update an existing connector.

Command: `run`

Example:

    $ bin/connect-cli run twitter-source <twitter.properties
    #Connector `twitter-source`:
    name=twitter-source
    tasks.max=1

    (snip)

    track.terms=test
    #task ids: 0

Diff a Connector
----------------------------

Diffs a connector config with the provided config.

Command: `diff`

Example:

    $ bin/connect-cli run twitter-source <twitter.properties
    #Connector `twitter-source`:
    name=twitter-source
    tasks.max=1

    (snip)

    track.terms=test
    #task ids: 0

Query Connector Status
----------------------

Shows a connector's status and the state of its tasks.

Command: `status`

Example:

    bin/connect-cli status my-toy-connector
    connectorState: RUNNING
    numberOfTasks: 3
    tasks:
      - taskId: 0
        taskState: RUNNING
      - taskId: 1
        taskState: FAILED
        trace: java.lang.Exception: broken on purpose
        at java.lang.Thread.run(Thread.java:745)
      - taskId: 2
        taskState: FAILED
        trace: java.lang.Exception: broken on purpose
        at java.lang.Thread.run(Thread.java:745)
        
Check which Plugins are on the Classpath and available in the Connect Cluster
-----------------------------------------------------------------------------
        
 Shows which Connector classes are available on the classpath.
        
Command: `plugins`
        
Example:  
        
        bin/connect-cli plugins
        Class name: com.datamountaineeer.streamreactor.connect.blockchain.source.BlockchainSourceConnector
        Class name: com.datamountaineer.streamreactor.connect.elastic.ElasticSinkConnector
        Class name: com.datamountaineer.streamreactor.connect.druid.DruidSinkConnector
        Class name: io.confluent.connect.hdfs.HdfsSinkConnector
        Class name: io.confluent.connect.jdbc.JdbcSourceConnector
        Class name: com.datamountaineer.streamreactor.connect.hbase.HbaseSinkConnector
        Class name: org.apache.kafka.connect.file.FileStreamSourceConnector
        Class name: com.datamountaineer.streamreactor.connect.hazelcast.sink.HazelCastSinkConnector
        Class name: com.datamountaineer.streamreactor.connect.cassandra.sink.CassandraSinkConnector
        Class name: com.datamountaineer.streamreactor.connect.rethink.source.ReThinkSourceConnector
        Class name: com.datamountaineer.streamreactor.connect.jms.sink.JMSSinkConnector
        Class name: com.datamountaineer.streamreactor.connect.influx.InfluxSinkConnector
        Class name: com.datamountaineer.streamreactor.connect.redis.sink.RedisSinkConnector
        Class name: com.datamountaineer.streamreactor.connect.bloomberg.BloombergSourceConnector
        Class name: com.datamountaineer.streamreactor.connect.yahoo.source.YahooSourceConnector
        Class name: com.datamountaineer.streamreactor.connect.kudu.sink.KuduSinkConnector
        Class name: org.apache.kafka.connect.file.FileStreamSinkConnector
        Class name: com.datamountaineer.streamreactor.connect.cassandra.source.CassandraSourceConnector
        Class name: com.datamountaineer.streamreactor.connect.voltdb.VoltSinkConnector
        Class name: com.datamountaineer.streamreactor.connect.mongodb.sink.MongoSinkConnector
        Class name: com.datamountaineer.streamreactor.connect.rethink.sink.ReThinkSinkConnector
        Class name: io.confluent.connect.hdfs.tools.SchemaSourceConnector
        
Describe the configuration of a Connector
-----------------------------------------

Describes the configuration parameters for a Connector.

Command: `describe`

Example:

    bin/connect-cli describe ReThinkSinkConnector
    {
      "name": "com.datamountaineer.streamreactor.connect.rethink.sink.ReThinkSinkConnector",
      "error_count": 3,
      "groups": ["Common", "Connection"],
      "configs": [{
        "definition": {
          "name": "connector.class",
          "display_name": "Connector class",
          "importance": "HIGH",
          "order": 2,
          "default_value": "",
          "dependents": [],
          "type": "STRING",
          "required": true,
          "group": "Common"
        },
        "value": {
          "name": "connector.class",
          "recommended_values": [],
          "errors": ["Missing required configuration \"connector.class\" which has no default value."],
          "visible": true
        }
      }, {
    ...........
    
 Validate a Connectors properties file against the required Configurations
 -------------------------------------------------------------------------
      
 Given a properties file for an instance of a Connector validate it against the Connector configuration.
      
 Command: `validate`
      
 Example: 
      
      bin/connect-cli validate ReThinkSinkConnector < ../conf/quickstarts/rethink-sink.properties
      ..............
         "definition": {
                "name": "connect.rethink.sink.port",
                "display_name": "connect.rethink.sink.port",
                "importance": "MEDIUM",
                "order": 3,
                "default_value": "28015",
                "dependents": [],
                "type": "INT",
                "required": false,
                "group": "Connection"
              },
              "value": {
                "name": "connect.rethink.sink.port",
                "visible": true,
                "errors": [],
                "recommended_values": [],
                "value": "28015"
              }
            }]
          }
          Validation failed.
          Missing required configuration "connect.rethink.sink.export.route.query" which has no default value.]: 
 
Pause a Connector
-----------------
   
Command: `pause`

Example: 
   
    bin/connect-cli pause cassandra-sink
    Waiting for pause
    connectorState:  RUNNING
    workerId: 10.0.0.9:8083
    numberOfTasks: 1
    tasks:
     - taskId: 0
       taskState: RUNNING
       workerId: 10.0.0.9:8083
       
Resume a Connector
------------------
   
Command: `resume`

Example: 
   
    bin/connect-cli resume cassandra-sink
    Waiting for resume
    connectorState:  RUNNING
    workerId: 10.0.0.9:8083
    numberOfTasks: 1
    tasks:
     - taskId: 0
       taskState: RUNNING
       workerId: 10.0.0.9:8083   
       
Restart a Connector
-------------------
   
Command: `restart`

Example: 
   
    bin/connect-cli restart cassandra-sink
    Waiting for restart
    connectorState:  RUNNING
    workerId: 10.0.0.9:8083
    numberOfTasks: 1
    tasks:
     - taskId: 0
       taskState: RUNNING
       workerId: 10.0.0.9:8083          

List all Tasks of a Connector
-----------------------------

Command: `task_ps`

Example:

    bin/connect-cli tasks_ps cassandra-sink
    - cassandra-sink task 0
      connector.class: com.datamountaineer.streamreactor.connect.cassandra.sink.CassandraSinkConnector
      bootstrap.servers: kafka-broker1:6667,kafka-broker2:6667,kafka-broker3:6667
      producer.schema.registry.url:http://schema-registry:8081
      ...
      
Get the status of a Connector Task
----------------------------------

Command: `task_status`

Example:

    bin/connect-cli task_status cassandra-sink 0
    taskId: 0
    taskState: RUNNING
    workerId: 10.0.0.9:8083
    
Restart a Connector Task
------------------------

Command: `task_restart`

Example:

    bin/connect-cli task_restart cassandra-sink 0

Misc
====

Contributions are encouraged, feedback to [rollulus](https://keybase.io/rollulus) at xs4all dot nl.

Thanks, enjoy!
