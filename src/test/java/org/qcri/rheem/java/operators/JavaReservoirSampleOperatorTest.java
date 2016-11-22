package org.qcri.rheem.java.operators;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.channels.JavaChannelInstance;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Test suite for {@link JavaReservoirSampleOperator}.
 */
public class JavaReservoirSampleOperatorTest extends JavaExecutionOperatorTestBase {

    @Test
    public void testExecution() {
        // Prepare test data.
        Stream<Integer> inputStream = Stream.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        final int sampleSize = 5;

        // Build the distinct operator.
        JavaReservoirSampleOperator<Integer> sampleOperator =
                new JavaReservoirSampleOperator<>(
                        iterationNumber -> sampleSize,
                        DataSetType.createDefaultUnchecked(Integer.class),
                        42
                );
        sampleOperator.setDatasetSize(10);

        JavaChannelInstance[] inputs = new JavaChannelInstance[]{createStreamChannelInstance(inputStream)};
        JavaChannelInstance[] outputs = new JavaChannelInstance[]{createCollectionChannelInstance()};

        // Execute.
        evaluate(sampleOperator, inputs, outputs);

        // Verify the outcome.
        final List<Integer> result = outputs[0].<Integer>provideStream().collect(Collectors.toList());
        Assert.assertEquals(sampleSize, result.size());
    }

}
