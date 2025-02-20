/*
 * MIT License
 *
 * Copyright (c) 2025 bakdata
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.bakdata.kafka.util;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.TopologyDescription.GlobalStore;
import org.apache.kafka.streams.TopologyDescription.Node;
import org.apache.kafka.streams.TopologyDescription.Processor;
import org.apache.kafka.streams.TopologyDescription.Sink;
import org.apache.kafka.streams.TopologyDescription.Source;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TableJoined;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.jooq.lambda.Seq;

/**
 * Representation of the nodes of a Kafka Streams topology
 */
public class TopologyInformation {
    private static final String SUBSCRIPTION_REGISTRATION_SUFFIX = "-subscription-registration-topic";
    private static final String SUBSCRIPTION_RESPONSE_SUFFIX = "-subscription-response-topic";
    private static final String CHANGELOG_SUFFIX = "-changelog";
    private static final String REPARTITION_SUFFIX = "-repartition";
    private static final String FILTER_SUFFIX = "-filter";
    /**
     * See
     * {@link org.apache.kafka.streams.kstream.internals.KTableImpl#doJoinOnForeignKey(KTable, Function, ValueJoiner, TableJoined, Materialized, boolean)}
     */
    private static final Collection<String> PSEUDO_TOPIC_SUFFIXES = Set.of("-pk", "-fk", "-vh");
    private final String streamsId;
    private final Collection<Node> nodes;
    private final Collection<GlobalStore> globalStores;

    /**
     * Create a new TopologyInformation of a topology and the unique app id
     *
     * @param topology topology to extract nodes from
     * @param streamsId unique app id to represent auto-created topics
     */
    public TopologyInformation(final Topology topology, final String streamsId) {
        this(topology.describe(), streamsId);
    }

    /**
     * Create a new TopologyInformation of a topology and the unique app id
     *
     * @param description topology description to extract nodes from
     * @param streamsId unique app id to represent auto-created topics
     */
    public TopologyInformation(final TopologyDescription description, final String streamsId) {
        this.nodes = getNodes(description);
        this.globalStores = description.globalStores();
        this.streamsId = streamsId;
    }

    private static List<Node> getNodes(final TopologyDescription description) {
        return description.subtopologies()
                .stream()
                .flatMap(subtopology -> subtopology.nodes().stream())
                .collect(Collectors.toList());
    }

    private static TopicSubscription toSubscription(final Source source) {
        final Set<String> topicSet = source.topicSet();
        return topicSet == null ? new PatternTopicSubscription(source.topicPattern())
                : new DirectTopicSubscription(topicSet);
    }

    private static Stream<String> createPseudoTopics(final String topic) {
        if (isSubscriptionRegistrationTopic(topic)) {
            return PSEUDO_TOPIC_SUFFIXES.stream().map(suffix -> String.format("%s%s", topic, suffix));
        }
        return Stream.empty();
    }

    private static String getRepartitionName(final Node processor) {
        final String name = processor.name();
        return name.substring(0, name.indexOf(FILTER_SUFFIX));
    }

    private static boolean isSubscriptionResponseTopic(final String topic) {
        return topic.endsWith(SUBSCRIPTION_RESPONSE_SUFFIX) || topic.contains("FK-JOIN-SUBSCRIPTION-RESPONSE");
    }

    private static boolean isSubscriptionRegistrationTopic(final String topic) {
        return topic.endsWith(SUBSCRIPTION_REGISTRATION_SUFFIX) || topic.contains("FK-JOIN-SUBSCRIPTION-REGISTRATION");
    }

    /**
     * Retrieve all topics associated with this topology that are auto-created by Kafka Streams
     *
     * @return list of internal topics
     */
    public List<String> getInternalTopics() {
        final Stream<String> internalSinks = this.getInternalSinks();
        final Stream<String> changelogTopics = this.getChangelogTopics();

        return Stream.concat(internalSinks, changelogTopics)
                .collect(Collectors.toList());
    }

    /**
     * Retrieve all sink topics associated with this topology that are not auto-created by Kafka Streams
     *
     * @return list of external sink topics
     */
    public List<String> getExternalSinkTopics() {
        return this.getAllTopics()
                .filter(this::isExternalTopic)
                .collect(Collectors.toList());
    }

    /**
     * Retrieve all source topics associated with this topology that are not auto-created by Kafka Streams
     *
     * @param allTopics list of all topics that exists in the Kafka cluster
     * @return list of external source topics
     */
    public List<String> getExternalSourceTopics(final Collection<String> allTopics) {
        final List<String> sinks = this.getExternalSinkTopics();
        return this.getSourceTopics(allTopics)
                .filter(t -> !sinks.contains(t))
                .collect(Collectors.toList());
    }

    /**
     * Retrieve all intermediate topics, i.e., topics that are both consumed and produced to/from, associated with this
     * topology that are not auto-created by Kafka Streams
     *
     * @param allTopics list of all topics that exists in the Kafka cluster
     * @return list of intermediate topics
     */
    public List<String> getIntermediateTopics(final Collection<String> allTopics) {
        final List<String> sinks = this.getExternalSinkTopics();
        return this.getSourceTopics(allTopics)
                .filter(sinks::contains)
                .collect(Collectors.toList());
    }

    public List<String> getSourceTopics() {
        return this.getAllSubscriptions()
                .map(TopicSubscription::getTopics)
                .flatMap(Collection::stream)
                .filter(this::isExternalTopic)
                .collect(Collectors.toList());
    }

    public List<Pattern> getSourcePatterns() {
        return this.getAllSubscriptions()
                .map(TopicSubscription::getPattern)
                .flatMap(Optional::stream)
                .collect(Collectors.toList());
    }

    /**
     * Retrieve all stores associated with this topology
     *
     * @return list of stores
     */
    public List<String> getStores() {
        return this.getAllStores()
                .collect(Collectors.toList());
    }

    /**
     * Retrieve all processors associated with this topology
     *
     * @return list of processors
     */
    public List<Processor> getProcessors() {
        return this.getAllProcessors()
                .collect(Collectors.toList());
    }

    /**
     * Retrieve all sources associated with this topology
     *
     * @return list of sources
     */
    public List<Source> getSources() {
        return this.getAllSources()
                .collect(Collectors.toList());
    }

    /**
     * Retrieve all sinks associated with this topology
     *
     * @return list of sinks
     */
    public List<Sink> getSinks() {
        return this.getAllSinks()
                .collect(Collectors.toList());
    }

    public Collection<String> getGlobalStoreSourceTopics() {
        return this.globalStores.stream()
                .map(GlobalStore::source)
                .map(TopologyInformation::toSubscription)
                .map(TopicSubscription::getTopics)
                .flatMap(Collection::stream)
                .filter(this::isExternalTopic)
                .collect(Collectors.toList());
    }

    private Stream<TopicSubscription> getAllSubscriptions() {
        return this.getAllSources()
                .map(TopologyInformation::toSubscription);
    }

    private Stream<Source> getAllSources() {
        return this.nodes.stream()
                .filter(Source.class::isInstance)
                .map(Source.class::cast);
    }

    private Stream<String> getAllTopics() {
        return this.getAllSinks()
                .map(Sink::topic)
                .filter(Objects::nonNull);
    }

    private Stream<Sink> getAllSinks() {
        return this.nodes.stream()
                .filter(Sink.class::isInstance)
                .map(Sink.class::cast);
    }

    private Stream<String> getAllStores() {
        return this.getAllProcessors()
                .flatMap(processor -> processor.stores().stream())
                .distinct();
    }

    private Stream<Processor> getAllProcessors() {
        return this.nodes.stream()
                .filter(Processor.class::isInstance)
                .map(Processor.class::cast);
    }

    private boolean isInternalTopic(final String topic) {
        if (topic.startsWith("KSTREAM-") || topic.startsWith("KTABLE-")) {
            return true;
        }
        if (topic.endsWith(CHANGELOG_SUFFIX)) {
            final List<String> changelogTopics = this.getChangelogTopics().collect(Collectors.toList());
            return changelogTopics.contains(topic);
        }
        if (topic.endsWith(REPARTITION_SUFFIX)) {
            final List<String> repartitionTopics = this.getRepartitionTopics().collect(Collectors.toList());
            return repartitionTopics.contains(topic);
        }
        if (isSubscriptionRegistrationTopic(topic)) {
            return true;
        }
        return isSubscriptionResponseTopic(topic);
    }

    private boolean isExternalTopic(final String topic) {
        return !this.isInternalTopic(topic);
    }

    private Stream<String> getInternalSinks() {
        return this.getAllTopics()
                .filter(this::isInternalTopic)
                .flatMap(topic -> Seq.of(topic).concat(createPseudoTopics(topic)))
                .map(topic -> String.format("%s-%s", this.streamsId, topic));
    }

    private Stream<String> getChangelogTopics() {
        return this.getAllStores()
                .map(store -> String.format("%s-%s%s", this.streamsId, store, CHANGELOG_SUFFIX));
    }

    private Stream<String> getRepartitionTopics() {
        return this.getAllProcessors()
                // internal repartitioning creates one processor that ends with "-repartition-filter",
                // one sink node, and one source node
                .filter(processor -> processor.name().endsWith(REPARTITION_SUFFIX + FILTER_SUFFIX))
                .map(TopologyInformation::getRepartitionName);
    }

    private Stream<String> getSourceTopics(final Collection<String> allTopics) {
        return this.getAllSubscriptions()
                .map(subscription -> subscription.resolveTopics(allTopics))
                .flatMap(Collection::stream)
                .filter(this::isExternalTopic);
    }
}
