Kafka Connect CLI
=================

This is a tiny command line interface (CLI) around the [Kafka Connect REST Interface](http://docs.confluent.io/2.0.1/connect/userguide.html#rest-interface) to manage connectors. It is used in a git like fashion where the first program argument indicates the command: it can be one of `[ls|get|rm|create|run]`.

The CLI is meant to behave as a good unix citizen: input from `stdin`; output to `stdout`; out of band info to `stderr` and non-zero exit status on error. Commands dealing with configuration expect or produce data in .properties style: `key=value` lines and comments start with a `#`.

    kafconcli 1.0
    Usage: kafconcli [ls|get|rm|create|run] [options] [<connector-name>...]

      --help
            prints this usage text
      -e <value> | --endpoint <value>
            Kafka REST URL, default is http://localhost:8083/

    Command: ls
    list active connectors names.

    Command: get
    get information about the specified connector(s).

    Command: rm
    remove the specified connector(s).

    Command: create
    create the specified connector with the .properties from stdin; the connector cannot already exist.

    Command: run
    create or update the specified connector with the .properties from stdin.

      <connector-name>...
            connector name(s)

Usage
=====

Clone this repository, do a `mvn package` and run the jar in a way you prefer, for example with the provided `cli` shell script. The CLI can be used as follows.

Get Active Connectors
---------------------

Command: `ls`

Example:

    $ ./cli ls
    twitter-source

Get Connector Information
-------------------------

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

Misc
====

Contributions are encouraged, feedback to [rollulus](https://keybase.io/rollulus) at xs4all dot nl, recommendations for more idiomatic Scala are welcome.

Thanks, enjoy!
