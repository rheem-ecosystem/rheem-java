package io.rheem.java.operators;

import io.rheem.basic.operators.DoWhileOperator;
import io.rheem.basic.operators.RepeatOperator;
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
 * Java implementation of the {@link DoWhileOperator}.
 */
public class JavaRepeatOperator<Type>
        extends RepeatOperator<Type>
        implements JavaExecutionOperator {

    /**
     * Keeps track of the current iteration number.
     */
    private int iterationCounter;

    /**
     * Creates a new instance.
     */
    public JavaRepeatOperator(int numIterations, DataSetType<Type> type) {
        super(numIterations, type);
    }

    public JavaRepeatOperator(RepeatOperator<Type> that) {
        super(that);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            JavaExecutor javaExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final ExecutionLineageNode executionLineageNode = new ExecutionLineageNode(operatorContext);
        executionLineageNode.addAtomicExecutionFromOperatorContext();

        final JavaChannelInstance input;
        switch (this.getState()) {
            case NOT_STARTED:
                assert inputs[INITIAL_INPUT_INDEX] != null;
                this.iterationCounter = 0;
                input = (JavaChannelInstance) inputs[INITIAL_INPUT_INDEX];
                break;
            case RUNNING:
                assert inputs[ITERATION_INPUT_INDEX] != null;
                this.iterationCounter++;
                input = (JavaChannelInstance) inputs[ITERATION_INPUT_INDEX];
                break;
            default:
                throw new IllegalStateException(String.format("%s is finished, yet executed.", this));

        }

        if (this.iterationCounter >= this.getNumIterations()) {
            // final loop output
            JavaExecutionOperator.forward(input, outputs[FINAL_OUTPUT_INDEX]);
            outputs[ITERATION_OUTPUT_INDEX] = null;
            this.setState(State.FINISHED);
        } else {
            outputs[FINAL_OUTPUT_INDEX] = null;
            JavaExecutionOperator.forward(input, outputs[ITERATION_OUTPUT_INDEX]);
            this.setState(State.RUNNING);
        }

        return executionLineageNode.collectAndMark();
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "rheem.java.repeat.load";
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new JavaRepeatOperator<>(this);
    }


    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        switch (index) {
            case INITIAL_INPUT_INDEX:
            case ITERATION_INPUT_INDEX:
                return Arrays.asList(StreamChannel.DESCRIPTOR, CollectionChannel.DESCRIPTOR);
            default:
                throw new IllegalStateException(String.format("%s has no %d-th input.", this, index));
        }
    }


    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(StreamChannel.DESCRIPTOR);
    }

}
