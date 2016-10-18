package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.operators.SampleOperator;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.costs.DefaultLoadEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.NestableLoadProfileEstimator;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.channels.CollectionChannel;
import org.qcri.rheem.java.channels.JavaChannelInstance;
import org.qcri.rheem.java.channels.StreamChannel;
import org.qcri.rheem.java.execution.JavaExecutor;

import java.util.*;
import java.util.function.Predicate;

/**
 * Java implementation of the {@link JavaRandomSampleOperator}. This sampling method is with replacement (i.e., duplicates may appear in the sample).
 */
public class JavaRandomSampleOperator<Type>
        extends SampleOperator<Type>
        implements JavaExecutionOperator {

    private final Random rand = new Random();

    /**
     * Creates a new instance.
     *
     * @param sampleSize size of sample
     */
    public JavaRandomSampleOperator(Integer sampleSize, DataSetType<Type> type) {
        super(sampleSize, type, Methods.RANDOM);
    }

    /**
     * Creates a new instance.
     *
     * @param sampleSize  size of sample
     * @param datasetSize size of data
     */
    public JavaRandomSampleOperator(Integer sampleSize, Long datasetSize, DataSetType<Type> type) {
        super(sampleSize, datasetSize, type, Methods.RANDOM);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public JavaRandomSampleOperator(SampleOperator<Type> that) {
        super(that);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Collection<OptimizationContext.OperatorContext> evaluate(ChannelInstance[] inputs,
                                                                    ChannelInstance[] outputs,
                                                                    JavaExecutor javaExecutor,
                                                                    OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        Long datasetSize = this.isDataSetSizeKnown() ? this.getDatasetSize() :
            ((CollectionChannel.Instance) inputs[0]).provideCollection().size();

        if (sampleSize >= datasetSize) { //return all
            ((StreamChannel.Instance) outputs[0]).accept(((JavaChannelInstance) inputs[0]).provideStream());
            return null;
        }

        final int[] sampleIndices = new int[sampleSize];
        final BitSet data = new BitSet();
        for (int i = 0; i < sampleSize; i++) {
            sampleIndices[i] = rand.nextInt(datasetSize.intValue());
            while (data.get(sampleIndices[i])) //without replacement
                sampleIndices[i] = rand.nextInt(datasetSize.intValue());
            data.set(sampleIndices[i]);
        }
        Arrays.sort(sampleIndices);

        ((StreamChannel.Instance) outputs[0]).accept(((JavaChannelInstance) inputs[0]).<Type>provideStream().filter(new Predicate<Type>() {
                    int streamIndex = 0;
                    int sampleIndex = 0;

                    @Override
                    public boolean test(Type element) {
                        if (sampleIndex == sampleIndices.length) //we already picked all our samples
                            return false;
                        if (streamIndex == sampleIndices[sampleIndex]) {
                            sampleIndex++;
                            streamIndex++;
                            return true;
                        }
                        streamIndex++;
                        return false;
                    }
                })
        );

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    @Override
    public Optional<LoadProfileEstimator<ExecutionOperator>> createLoadProfileEstimator(Configuration configuration) {
        return Optional.of(new NestableLoadProfileEstimator<>(
                new DefaultLoadEstimator<>(this.getNumInputs(), 1, 0.9d, (inCards, outCards) -> 25 * inCards[0] + 350000),
                LoadEstimator.createFallback(this.getNumInputs(), 1)
        ));
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new JavaRandomSampleOperator<>(this.sampleSize, this.getType());
    }


    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        return this.isDataSetSizeKnown() ?
            Arrays.asList(CollectionChannel.DESCRIPTOR, StreamChannel.DESCRIPTOR) :
            Collections.singletonList(CollectionChannel.DESCRIPTOR);

    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(StreamChannel.DESCRIPTOR);
    }

}
