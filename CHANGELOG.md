# Change Log

## [2.1.0](https://github.com/bakdata/fluent-kafka-streams-tests/tree/2.1.0) (2020-01-21)
[Full Changelog](https://github.com/bakdata/fluent-kafka-streams-tests/compare/2.0.4...2.1.0)

**Merged pull requests:**

- Update Kafka to 2.4.0 [\#38](https://github.com/bakdata/fluent-kafka-streams-tests/pull/38) ([@philipp94831](https://github.com/philipp94831))

## [2.0.4](https://github.com/bakdata/fluent-kafka-streams-tests/tree/2.0.4) (2019-11-07)
[Full Changelog](https://github.com/bakdata/fluent-kafka-streams-tests/compare/2.0.3...2.0.4)

**Merged pull requests:**

- Fix schema registry behavior if a subject does not exist or has been deleted [\#36](https://github.com/bakdata/fluent-kafka-streams-tests/pull/36) ([@philipp94831](https://github.com/philipp94831))

## [2.0.3](https://github.com/bakdata/fluent-kafka-streams-tests/tree/2.0.3) (2019-10-30)
[Full Changelog](https://github.com/bakdata/fluent-kafka-streams-tests/compare/2.0.2...2.0.3)

**Closed issues:**

- fluent\-kafka\-streams\-tests\-junit4 for JDK 8 [\#33](https://github.com/bakdata/fluent-kafka-streams-tests/issues/33)

**Merged pull requests:**

- Add support for deleteSubject and getAllSubjects [\#35](https://github.com/bakdata/fluent-kafka-streams-tests/pull/35) ([@torbsto](https://github.com/torbsto))

## [2.0.2](https://github.com/bakdata/fluent-kafka-streams-tests/tree/2.0.2) (2019-10-11)
[Full Changelog](https://github.com/bakdata/fluent-kafka-streams-tests/compare/2.0.1...2.0.2)

**Closed issues:**

- \[schema\-registry\] Topology with manually created SpecificAvroSerde needs to know the schema\-registry URL upfront [\#30](https://github.com/bakdata/fluent-kafka-streams-tests/issues/30)

**Merged pull requests:**

- Exclude repartition topics [\#34](https://github.com/bakdata/fluent-kafka-streams-tests/pull/34) ([@torbsto](https://github.com/torbsto))
- Fix GlobalKTable Sources [\#32](https://github.com/bakdata/fluent-kafka-streams-tests/pull/32) ([@torbsto](https://github.com/torbsto))

## [2.0.1](https://github.com/bakdata/fluent-kafka-streams-tests/tree/2.0.1) (2019-06-09)
[Full Changelog](https://github.com/bakdata/fluent-kafka-streams-tests/compare/2.0.0...2.0.1)

**Closed issues:**

- \[schema\-registry\] MockedSchemaRegistry 5.2.1 returns Schema id \-1 for some Schema registrations [\#28](https://github.com/bakdata/fluent-kafka-streams-tests/issues/28)

**Merged pull requests:**

- Downgraded confluent version to 5.1.3 to avoid schema registry bug as… [\#29](https://github.com/bakdata/fluent-kafka-streams-tests/pull/29) ([@AHeise](https://github.com/AHeise))

## [2.0.0](https://github.com/bakdata/fluent-kafka-streams-tests/tree/2.0.0) (2019-05-14)
[Full Changelog](https://github.com/bakdata/fluent-kafka-streams-tests/compare/1.1.0...2.0.0)

**Merged pull requests:**

- Extract junit support [\#27](https://github.com/bakdata/fluent-kafka-streams-tests/pull/27) ([@AHeise](https://github.com/AHeise))

## [1.1.0](https://github.com/bakdata/fluent-kafka-streams-tests/tree/1.1.0) (2019-04-30)
[Full Changelog](https://github.com/bakdata/fluent-kafka-streams-tests/compare/1.0.1...1.1.0)

**Closed issues:**

- \[schema\-registry\] Use with Java 8 tests [\#21](https://github.com/bakdata/fluent-kafka-streams-tests/issues/21)
- Schema Registry List and Get subject versions [\#19](https://github.com/bakdata/fluent-kafka-streams-tests/issues/19)
- Bump kafka version to 2.2.0 [\#18](https://github.com/bakdata/fluent-kafka-streams-tests/issues/18)
- Upate Medium Links [\#14](https://github.com/bakdata/fluent-kafka-streams-tests/issues/14)

**Fixed bugs:**

- java.lang.ClassNotFoundException: com.fasterxml.jackson.annotation.JsonMerge [\#23](https://github.com/bakdata/fluent-kafka-streams-tests/issues/23)

**Merged pull requests:**

- Pinning jackson version to avoid conflict between wiremock and schema… [\#26](https://github.com/bakdata/fluent-kafka-streams-tests/pull/26) ([@AHeise](https://github.com/AHeise))
- Set java version of SR mock to java 8 [\#24](https://github.com/bakdata/fluent-kafka-streams-tests/pull/24) ([@AHeise](https://github.com/AHeise))
- \(\#19\) Mock subject version endpoints [\#20](https://github.com/bakdata/fluent-kafka-streams-tests/pull/20) ([@cricket007](https://github.com/cricket007))

## [1.0.1](https://github.com/bakdata/fluent-kafka-streams-tests/tree/1.0.1) (2019-02-22)
[Full Changelog](https://github.com/bakdata/fluent-kafka-streams-tests/compare/1.0.0...1.0.1)

**Merged pull requests:**

- General code cleanup [\#16](https://github.com/bakdata/fluent-kafka-streams-tests/pull/16) ([@AHeise](https://github.com/AHeise))
- Add README for Schema Registry Mock [\#13](https://github.com/bakdata/fluent-kafka-streams-tests/pull/13) ([@lawben](https://github.com/lawben))
- Improved deployment fault tolerance [\#15](https://github.com/bakdata/fluent-kafka-streams-tests/pull/15) ([@AHeise](https://github.com/AHeise))

## [1.0.0](https://github.com/bakdata/fluent-kafka-streams-tests/tree/1.0.0) (2019-02-21)

**Fixed bugs:**

- Parellel test execution may result in conflicts in the state directory [\#7](https://github.com/bakdata/fluent-kafka-streams-tests/issues/7)

**Merged pull requests:**

- Using temp state dir to isolate test execution [\#8](https://github.com/bakdata/fluent-kafka-streams-tests/pull/8) ([@AHeise](https://github.com/AHeise))
- Fix javadoc setup and build [\#12](https://github.com/bakdata/fluent-kafka-streams-tests/pull/12) ([@AHeise](https://github.com/AHeise))
- Fix javadoc setup and build [\#11](https://github.com/bakdata/fluent-kafka-streams-tests/pull/11) ([@AHeise](https://github.com/AHeise))
- Revert "Fix javadoc setup" [\#10](https://github.com/bakdata/fluent-kafka-streams-tests/pull/10) ([@AHeise](https://github.com/AHeise))
- Fix javadoc setup [\#9](https://github.com/bakdata/fluent-kafka-streams-tests/pull/9) ([@AHeise](https://github.com/AHeise))
- Add More Tests [\#6](https://github.com/bakdata/fluent-kafka-streams-tests/pull/6) ([@lawben](https://github.com/lawben))
- Add javadocs for public interface [\#3](https://github.com/bakdata/fluent-kafka-streams-tests/pull/3) ([@lawben](https://github.com/lawben))
- Refactored schema registry mock [\#5](https://github.com/bakdata/fluent-kafka-streams-tests/pull/5) ([@AHeise](https://github.com/AHeise))
- Add README [\#1](https://github.com/bakdata/fluent-kafka-streams-tests/pull/1) ([@lawben](https://github.com/lawben))
- Simplified avro example for blog article [\#4](https://github.com/bakdata/fluent-kafka-streams-tests/pull/4) ([@AHeise](https://github.com/AHeise))
- Fix build and badges [\#2](https://github.com/bakdata/fluent-kafka-streams-tests/pull/2) ([@AHeise](https://github.com/AHeise))
