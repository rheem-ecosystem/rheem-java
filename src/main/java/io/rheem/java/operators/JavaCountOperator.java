package io.rheem.java.operators;

import io.rheem.basic.operators.CountOperator;
import io.rheem.core.optimizer.OptimizationContext;
import io.rheem.core.plan.rheemplan.ExecutionOperator;
import io.rheem.core.platform.ChannelDescriptor;
import io.rheem.core.platform.ChannelInstance;
import io.rheem.core.platform.lineage.ExecutionLineageNode;
import io.rheem.core.types.DataSetType;
import io.rheem.core.util.Tuple;
import io.rheem.java.channels.CollectionChannel;
import io.rheem.java.channels.JavaChannelInstance;
import io.rheem.java.channels.StreamChannel;
import io.rheem.java.execution.JavaExecutor;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Java implementation of the {@link CountOperator}.
 */
public class JavaCountOperator<Type>
        extends CountOperator<Type>
        implements JavaExecutionOperator {


    /**
     * Creates a new instance.
     *
     * @param type type of the dataset elements
     */
    public JavaCountOperator(DataSetType<Type> type) {
        super(type);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public JavaCountOperator(CountOperator<Type> that) {
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

        final JavaChannelInstance input = (JavaChannelInstance) inputs[0];
        final long count;
        if (input instanceof CollectionChannel.Instance) {
            count = ((CollectionChannel.Instance) input).provideCollection().size();
        } else {
            count = input.provideStream().count();
        }
        ((CollectionChannel.Instance) outputs[0]).accept(Collections.singleton(count));

        return ExecutionOperator.modelEagerExecution(inputs, outputs, operatorContext);
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "rheem.java.count.load";
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new JavaCountOperator<>(this.getInputType());
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
