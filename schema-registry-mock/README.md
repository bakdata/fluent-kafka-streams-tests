[![Build Status](https://travis-ci.com/bakdata/fluent-kafka-streams-tests.svg?branch=master)](https://travis-ci.com/bakdata/fluent-kafka-streams-tests)
[![Sonarcloud status](https://sonarcloud.io/api/project_badges/measure?project=com.bakdata.fluent-kafka-streams-tests%3Afluent-kafka-streams-tests&metric=alert_status)](https://sonarcloud.io/dashboard?id=com.bakdata.fluent-kafka-streams-tests%3Afluent-kafka-streams-tests)
[![Code coverage](https://sonarcloud.io/api/project_badges/measure?project=com.bakdata.fluent-kafka-streams-tests%3Afluent-kafka-streams-tests&metric=coverage)](https://sonarcloud.io/dashboard?id=com.bakdata.fluent-kafka-streams-tests%3Afluent-kafka-streams-tests)
[![Maven](https://img.shields.io/maven-central/v/com.bakdata.fluent-kafka-streams-tests/schema-registry-mock.svg)](https://search.maven.org/search?q=g:com.bakdata.fluent-kafka-streams-tests%20AND%20a:schema-registry-mock&core=gav)

Schema Registry Mock
====================

Mock your Schema Registry in Kafka Streams Tests.

You can find a [blog post on medium](https://medium.com/bakdata/transparent-schema-registry-for-kafka-streams-6b43a3e7a15c) with some examples and detailed explanations of how the Schema Registry Mock works with the Fluent Kafka Streams Tests framework.

## Getting Started
You can find the Schema Registry Mock via Maven Central. 

#### Gradle
```gradle
compile group: 'com.bakdata', name: 'schema-registry-mock-junit5', version: '2.0.0'
```

#### Maven
```xml
<dependency>
    <groupId>com.bakdata</groupId>
    <artifactId>schema-registry-mock-junit5</artifactId>
    <version>2.0.0</version>
</dependency>
```

There is also a junit4 version and one without any dependencies to a specific testing framework.

For other build tools or versions, refer to the [latest version in MvnRepository](https://mvnrepository.com/artifact/com.bakdata.fluent-kafka-streams-tests/latest).

## Using it in Tests

There are two ways to use the Mock Schema Registry, 
together with the Fluent Kafka Streams Tests 
or as a standalone module in your existing test framework.

### With Fluent Kafka Streams Tests

Using the Mock Schema Registry with the Fluent Kafka Streams Tests is very straightforward.
All you need to do, is set your (default) serde to `GenericAvroSerde` or `SpecificAvroSerde`, so that your topology actually needs to use the Schema Registry.
The `TestTopology` takes care of updating the Kafka config with the url of the Mock Schema Registry for you.

You can then write a simple test that uses Avro, without having to deal with the Schema Registry.

```java
@Test
void shouldAggregateInhabitants() {
  this.testTopology.input()
    .add(new Person("Huey", "City1"))
    .add(new Person("Dewey", "City2"))
    .add(new Person("Louie", "City1"));

  this.testTopology.tableOutput().withValueType(City.class)
    .expectNextRecord().hasKey("City1").hasValue(new City("City1", 2))
    .expectNextRecord().hasKey("City2").hasValue(new City("City2", 1))
    .expectNoMoreRecord();
}
```

 
### As a Standalone Module 
To use this in your tests, you need to do three things: 

- Setup the `SchemaRegistryMock` as an extension of your test.
```java
class SchemaRegistryMockTest {
    @RegisterExtension
    final SchemaRegistryMockExtension schemaRegistry = new SchemaRegistryMockExtension();
    ...
}
```
You can also use the junit4 rule or set it up manually by using the version without framework dependencies.
 - Set the serde of your key and/or value to `GenericAvroSerde` or `SpecificAvroSerde`, so that your topology actually needs to use the Schema Registry.
 - Set the `AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG` to `schemaRegistry.getSchemaRegistryUrl()`, so that your test knows where to find the Schema Registry.
```java
@Test
void customTest() {
  Properties properties = new Properties();
  properties.setProperty(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, this.getSchemaRegistryUrl());
  ...
  testDriver = new TopologyTestDriver(topology, properties);
}
```

After that, you can write a test that uses the Schema Registry in your testing framework. 


## Development

If you want to contribute to this project, you can simply clone the repository and build it via Gradle.
All dependencies should be included in the Gradle files, there are no external prerequisites.

```bash
> git clone git@github.com:bakdata/fluent-kafka-streams-tests.git
> cd fluent-kafka-streams-tests && ./gradlew build
```

Please note, that we have [code styles](https://github.com/bakdata/bakdata-code-styles) for Java.
They are basically the Google style guide, with some small modifications.

## Contributing

We are happy if you want to contribute to this project.
If you find any bugs or have suggestions for improvements, please open an issue.
We are also happy to accept your PRs.
Just open an issue beforehand and let us know what you want to do and why.

## License
This project is licensed under the MIT license.
Have a look at the [LICENSE](https://github.com/bakdata/fluent-kafka-streams-tests/blob/master/LICENSE) for more details.
