[![Stories in Ready](https://badge.waffle.io/datamountaineer/kafka-connect-tools.png?label=ready&title=Ready)](https://waffle.io/datamountaineer/kafka-connect-tools)
[![Build Status](https://travis-ci.org/datamountaineer/kafka-connect-tools.svg?branch=master)](https://travis-ci.org/datamountaineer/kafka-connect-tools)
[<img src="https://img.shields.io/badge/latest%20release-v0.2-blue.svg?label=latest%20release"/>](http://search.maven.org/#search%7Cga%7C1%7Ca%3A%22kafka-connect-cli%22)

Connect tools is Maven

```bash
<dependency>
	<groupId>com.datamountaineer</groupId>
	<artifactId>kafka-connect-cli</artifactId>
	<version>0.2</version>
</dependency>
```

##Requirements

* Java 1.8

Kafka Connect CLI
=================

This is a tiny command line interface (CLI) around the [Kafka Connect REST Interface](http://docs.confluent.io/2.0.1/connect/userguide.html#rest-interface) to manage connectors. It is used in a git like fashion where the first program argument indicates the command: it can be one of `[ps|get|rm|create|run]`.

The CLI is meant to behave as a good unix citizen: input from `stdin`; output to `stdout`; out of band info to `stderr` and non-zero exit status on error. Commands dealing with configuration expect or produce data in .properties style: `key=value` lines and comments start with a `#`.

    kafka-connect-cli 0.4
    Usage: kafka-connect-cli [ps|get|rm|create|run|status] [options] [<connector-name>]

      --help
            prints this usage text
      -e <value> | --endpoint <value>
            Kafka Connect REST URL, default is http://localhost:8083/

    Command: ps
    list active connectors names.

    Command: get
    get the configuration of the specified connector.

    Command: rm
    remove the specified connector.

    Command: create
    create the specified connector with the .properties from stdin; the connector cannot already exist.

    Command: run
    create or update the specified connector with the .properties from stdin.

    Command: status
    get connector and it's task(s) state(s).

      <connector-name>
            connector name

You can override the default endpoint by setting an environment variable `KAFKA_CONNECT_REST` i.e.

    export KAFKA_CONNECT_REST="http://myserver:myport"

To Build
========

```bash
gradle fatJar
```


Usage
=====

Clone this repository, do a `mvn package` and run the jar in a way you prefer, for example with the provided `cli` shell script. The CLI can be used as follows.

Get Active Connectors
---------------------

Command: `ps`

Example:

    $ ./cli ps
    twitter-source

Get Connector Configuration
---------------------------

Command: `get`

Example:

    $ ./cli get twitter-source
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

    $ ./cli rm twitter-source

Create a New Connector
----------------------

The connector cannot already exist.

Command: `create`

Example:

    $ ./cli create twitter-source <twitter.properties
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

    $ ./cli run twitter-source <twitter.properties
    #Connector `twitter-source`:
    name=twitter-source
    tasks.max=1

    (snip)

    track.terms=test
    #task ids: 0

Query Connector State
---------------------

Shows a connector's state and the state of its tasks.

Command: `status`

Example:

    ./cli status my-toy-connector
    connectorState: RUNNING
    numberOfTasks: 2
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

Misc
====

Contributions are encouraged, feedback to [rollulus](https://keybase.io/rollulus) at xs4all dot nl.

Thanks, enjoy!
