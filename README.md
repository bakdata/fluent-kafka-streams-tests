[![Build status](https://travis-ci.org/bakdata/fluent-kafka-streams-tests.svg?branch=master)](https://travis-ci.org/bakdata/fluent-kafka-streams-tests/) 
[![Sonarcloud status](https://sonarcloud.io/api/project_badges/measure?project=bakdata-fluent-kafka-streams-tests&metric=alert_status)](https://sonarcloud.io/dashboard?id=bakdata-fluent-kafka-streams-tests)
[![Code coverage](https://sonarcloud.io/api/project_badges/measure?project=bakdata-fluent-kafka-streams-tests&metric=coverage)](https://sonarcloud.io/dashboard?id=bakdata-fluent-kafka-streams-tests)

Fluent Kafka Stream Tests
=========================

Write clean and concise tests for your Kafka Streams application.

You can find a [blog post on medium](https://medium.com/bakdata/xxx) with some examples and detailed explanations of how Fluent Kafka Stream Tests works.

## Getting Started

You can add Fluent Kafka Stream Tests via Maven Central.

#### Gradle
```gradle
compile group: 'com.bakdata.gradle', name: 'XXX', version: '1.0.0'
```

#### Maven
```xml
<dependency>
    <groupId>com.bakdata.gradle</groupId>
    <artifactId>XXX</artifactId>
    <version>1.0.0</version>
</dependency>
```

### Using it to Write Tests

Here are two example tests which show you how to use Fluent Kafka Stream Tests.

#### Word Count Test
Assume you have a Word Count Kafka Stream application, called `WordCount`, and want to test it correctly.
First, start by creating a new test class with your application.

```java
class WordCountTest {
    private final WordCount app = new WordCount();
}
```

Then, set up the `TestTopology`.

```java
class WordCountTest {
    private final WordCount app = new WordCount();

    @RegisterExtension
    final TestTopology<Object, String> testTopology =
        new TestTopology<>(this.app::getTopology, this.app.getKafkaProperties());
}
```

The `TestTopology` takes care of all the inputs, processing, and outputs of you application.
For it to do that, you need to register it a an extension (JUnit5), so certain setup/teardown methods are called.
The constructor expects a topology factory (for a fresh topology in each test) that creates the topology under test.

Additionally, the properties of the `KafkaClient` need to be specified.
Broker and application-id must be present (Kafka testutil limitation), but are ignored.
Most importantly, if the application expects default serde for key and value, these must be present in the properties or explicitly specified with `withDefaultKeySerde(Serde serde)` and/or `withDefaultValueSerde(Serde serde)`.

To test your appliction, you can simply write a JUnit test.
```java
class WordCountTest {
    private final WordCount app = new WordCount();

    @RegisterExtension
    final TestTopology<Object, String> testTopology =
        new TestTopology<>(this.app::getTopology, this.app.getKafkaProperties());

    @Test
    void shouldAggregateSameWordStream() {
        this.testTopology.input()
            .add("bla")
            .add("blub")
            .add("bla");

        this.testTopology.streamOutput().withSerde(Serdes.String(), Serdes.Long())
            .expectNextRecord().hasKey("bla").hasValue(1L)
            .expectNextRecord().hasKey("blub").hasValue(1L)
            .expectNextRecord().hasKey("bla").hasValue(2L)
            .expectNoMoreRecord();
    }
}
```


The `TestTopology` has a method `.input()` to retrieve the input topic (or `.input(String topic)`) if more than one input topic is present).
You can simply add values to your input stream by calling `.add(V value)` or `.add(K key, V value)`.

To get the output, `TestTopology` provides two methods: `.streamOutput()` and `.tableOutput()`.
They behave just like the input with regard to the number of output topics.
Using the stream version simulates Kafka's stream-semantics, meaning that a key can be present many times in an output stream, whereas the table-semantics only output the newest value of each key.

To check the output records, you can call `.expectNextRecord()` to indicate that the output should not be empty.
You can then inspect the record with `.hasKey(K key)` and `.hasValue(V value)`.
Both are optional, but highly recommended so that your output is always valid.

Once you expect no further records, call `.expectNoMoreRecord()` to indicate the end of the output stream.

#### Using Other Test Frameworks to Check Output
We intentionally kept the API for output checking slim, because there are many tools out there which focus on doing exactly that.
The `TestOutput` class implements the `Iterable` interface, so you can use your favorite tool to test iterables.

Here is an example using [AssertJ](http://joel-costigliola.github.io/assertj/).

```java
@Test
void shouldReturnCorrectIteratorTable() {
    this.testTopology.input()
        .add("bla")
        .add("blub")
        .add("foo");

    final List<String> expected = List.of("bla", "blub", "foo");
    assertThat(this.testTopology.tableOutput().withSerde(Serdes.String(), Serdes.Long()))
        .extracting(ProducerRecord::key)
        .containsAll(expected);
}
```

#### More Examples

You can find many more tests in [this repository's test code](https://github.com/bakdata/fluent-kafka-streams-tests/tree/master/fluent-kafka-streams-tests/src/test/java/com/bakdata/fluent_kafka_streams_tests).


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

## Acknowledgements

