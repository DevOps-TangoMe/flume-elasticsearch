flume-elasticsearch
===========

Flume Elasticsearch Sink Serializer.

This adds the ability to specify the index format through the "indexFormat" parameter


License
-------

This is released under Apache License v2


Example configuration
---------------------

Example sink configuration:

  agent.sinks.elasticsearch.type = elasticsearch
  agent.sinks.elasticsearch.hostNames = 127.0.0.1
  agent.sinks.elasticsearch.indexName = logstash
  agent.sinks.elasticsearch.clusterName = logstash
  agent.sinks.elasticsearch.batchSize = 5000
  agent.sinks.elasticsearch.ttl = 2
  agent.sinks.elasticsearch.serializer = com.tango.logstash.flume.elasticsearch.sink.LogstashEventSerializerIndexRequestBuilderFactory
  agent.sinks.elasticsearch.serializer.indexFormat = yyyy.MM.dd-HH


Building
--------

This project uses maven for building all the artefacts.
You can build it with the following command:
    mvn clean install

This will build the following artefacts:
* flume-elasticsearch-dist/target/flume-elasticsearch-1.0.0-SNAPSHOT-dist.tar.gz
  The tarball can be directly unpacked into Apache Flume plugins.d directory

* flume-elasticsearch-dist/target/rpm/tango-flume-elasticsearch/RPMS/noarch/tango-flume-elasticsearch-1.0.0-SNAPSHOT*.noarch.rpm
  This package will install itself on top of Apache Flume package and be ready for use right away.



