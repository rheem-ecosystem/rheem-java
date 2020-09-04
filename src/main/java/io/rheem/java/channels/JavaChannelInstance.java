package io.rheem.java.channels;

import io.rheem.core.plan.executionplan.Channel;
import io.rheem.core.platform.ChannelInstance;

import java.util.stream.Stream;

/**
 * Defines execution logic to handle a {@link Channel}.
 */
public interface JavaChannelInstance extends ChannelInstance {

    /**
     * Provide the producer's result to a consumer.
     *
     * @return the producer's result
     */
    <T> Stream<T> provideStream();

}
