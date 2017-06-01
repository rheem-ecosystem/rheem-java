package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.operators.CoGroupOperator;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimators;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.java.channels.CollectionChannel;
import org.qcri.rheem.java.channels.JavaChannelInstance;
import org.qcri.rheem.java.channels.StreamChannel;
import org.qcri.rheem.java.execution.JavaExecutor;

import java.util.*;
import java.util.function.Function;

/**
 * Java implementation of the {@link CoGroupOperator}.
 */
public class JavaCoGroupOperator<InputType0, InputType1, KeyType>
        extends CoGroupOperator<InputType0, InputType1, KeyType>
        implements JavaExecutionOperator {

    /**
     * Creates a new instance.
     */
    public JavaCoGroupOperator(DataSetType<InputType0> inputType0,
                               DataSetType<InputType1> inputType1,
                               TransformationDescriptor<InputType0, KeyType> keyDescriptor0,
                               TransformationDescriptor<InputType1, KeyType> keyDescriptor1) {

        super(keyDescriptor0, keyDescriptor1, inputType0, inputType1);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public JavaCoGroupOperator(CoGroupOperator<InputType0, InputType1, KeyType> that) {
        super(that);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            JavaExecutor javaExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final Function<InputType0, KeyType> keyExtractor0 = javaExecutor.getCompiler().compile(this.keyDescriptor0);
        final Function<InputType1, KeyType> keyExtractor1 = javaExecutor.getCompiler().compile(this.keyDescriptor1);

        // Group input 0.
        final CardinalityEstimate cardinalityEstimate0 = operatorContext.getInputCardinality(0);
        int expectedNumElements0 = (int) cardinalityEstimate0.getGeometricMeanEstimate();
        Map<KeyType, Collection<InputType0>> groups0 = new HashMap<>(expectedNumElements0);
        ((JavaChannelInstance) inputs[0]).<InputType0>provideStream().forEach(dataQuantum0 ->
                groups0.compute(keyExtractor0.apply(dataQuantum0),
                        (key, value) -> {
                            value = value == null ? new LinkedList<>() : value;
                            value.add(dataQuantum0);
                            return value;
                        }
                )
        );

        // Group input 1.
        final CardinalityEstimate cardinalityEstimate1 = operatorContext.getInputCardinality(1);
        int expectedNumElements1 = (int) cardinalityEstimate1.getGeometricMeanEstimate();
        Map<KeyType, Collection<InputType1>> groups1 = new HashMap<>(expectedNumElements1);
        ((JavaChannelInstance) inputs[1]).<InputType1>provideStream().forEach(dataQuantum1 ->
                groups1.compute(keyExtractor1.apply(dataQuantum1),
                        (key, value) -> {
                            value = value == null ? new LinkedList<>() : value;
                            value.add(dataQuantum1);
                            return value;
                        }
                )
        );

        // Create the co-groups.
        Collection<Tuple2<Iterable<InputType0>, Iterable<InputType1>>> coGroups =
                new ArrayList<>(expectedNumElements0 + expectedNumElements1);
        for (Map.Entry<KeyType, Collection<InputType0>> entry : groups0.entrySet()) {
            Collection<InputType0> group0 = entry.getValue();
            Collection<InputType1> group1 = groups1.remove(entry.getKey());
            coGroups.add(new Tuple2<>(
                    group0,
                    group1 == null ? Collections.emptyList() : group1
            ));
        }
        for (Collection<InputType1> group1 : groups1.values()) {
            coGroups.add(new Tuple2<>(Collections.emptyList(), group1));
        }
        ((CollectionChannel.Instance) outputs[0]).accept(coGroups);

        return ExecutionOperator.modelEagerExecution(inputs, outputs, operatorContext);
    }

    @Override
    public Collection<String> getLoadProfileEstimatorConfigurationKeys() {
        return Collections.singletonList("rheem.java.cogroup.load");
    }

    @Override
    public Optional<LoadProfileEstimator> createLoadProfileEstimator(Configuration configuration) {
        final Optional<LoadProfileEstimator> optEstimator =
                JavaExecutionOperator.super.createLoadProfileEstimator(configuration);
        LoadProfileEstimators.nestUdfEstimator(optEstimator, this.keyDescriptor0, configuration);
        LoadProfileEstimators.nestUdfEstimator(optEstimator, this.keyDescriptor1, configuration);
        return optEstimator;
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new JavaCoGroupOperator<>(this.getInputType0(), this.getInputType1(),
                this.getKeyDescriptor0(), this.getKeyDescriptor1());
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        return Arrays.asList(CollectionChannel.DESCRIPTOR, StreamChannel.DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(CollectionChannel.DESCRIPTOR);
    }

}
