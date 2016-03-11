Kafka Connect CLI
=================

A tiny CLI around the [Kafka Connect REST Interface](http://docs.confluent.io/2.0.1/connect/userguide.html#rest-interface) to manage connectors.

Work in progress.

Examples of intended behaviour:

    ./kafconcli ls
    twitter-source

    ./kafconcli info twitter-source
    twitter-source:
      config:
        name: twitter-source
        tasks.max: 1
        batch.timeout: 100e-3
        connector.class: com.eneco.trading.kafka.connect.twitter.TwitterSourceConnector
        twitter.token: xxx
        twitter.consumersecret: xxx
        twitter.consumerkey: xxx
        topic: tweets
        twitter.secret: xxx
        batch.size: 100
        track.terms: test
      task ids: 0

    ./kafconcli rm twitter-source

    ./kafconcli info twitter-source
    error: Connector twitter-source not found

