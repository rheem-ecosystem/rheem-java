package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.operators.LoopOperator;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.optimizer.OptimizationContext;
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
import java.util.function.Predicate;

/**
 * Java implementation of the {@link LoopOperator}.
 */
public class JavaLoopOperator<InputType, ConvergenceType>
        extends LoopOperator<InputType, ConvergenceType>
        implements JavaExecutionOperator {


    /**
     * Creates a new instance.
     */
    public JavaLoopOperator(DataSetType<InputType> inputType,
                            DataSetType<ConvergenceType> convergenceType,
                            PredicateDescriptor.SerializablePredicate<Collection<ConvergenceType>> criterionPredicate,
                            int numExpectedIterations) {
        super(inputType, convergenceType, criterionPredicate, numExpectedIterations);
    }

    public JavaLoopOperator(DataSetType<InputType> inputType,
                            DataSetType<ConvergenceType> convergenceType,
                            PredicateDescriptor<Collection<ConvergenceType>> criterionDescriptor,
                            int numExpectedIterations) {
        super(inputType, convergenceType, criterionDescriptor, numExpectedIterations);
    }

    /**
     * Creates a copy of the given {@link LoopOperator}.
     *
     * @param that should be copied
     */
    public JavaLoopOperator(LoopOperator<InputType, ConvergenceType> that) {
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

        final Predicate<Collection<ConvergenceType>> stoppingCondition =
                javaExecutor.getCompiler().compile(this.criterionDescriptor);
        JavaExecutor.openFunction(this, stoppingCondition, inputs, operatorContext);

        boolean endloop = false;
        final Collection<ConvergenceType> convergenceCollection;
        final JavaChannelInstance input;
        switch (this.getState()) {
            case NOT_STARTED:
                assert inputs[INITIAL_INPUT_INDEX] != null;
                assert inputs[INITIAL_CONVERGENCE_INPUT_INDEX] != null;

                input = (JavaChannelInstance) inputs[INITIAL_INPUT_INDEX];
                JavaExecutionOperator.forward(inputs[INITIAL_CONVERGENCE_INPUT_INDEX], outputs[ITERATION_CONVERGENCE_OUTPUT_INDEX]);
                break;
            case RUNNING:
                assert inputs[ITERATION_INPUT_INDEX] != null;
                assert inputs[ITERATION_CONVERGENCE_INPUT_INDEX] != null;

                input = (JavaChannelInstance) inputs[ITERATION_INPUT_INDEX];
                convergenceCollection = ((CollectionChannel.Instance) inputs[ITERATION_CONVERGENCE_INPUT_INDEX]).provideCollection();

                endloop = stoppingCondition.test(convergenceCollection);

                JavaExecutionOperator.forward(inputs[ITERATION_CONVERGENCE_INPUT_INDEX], outputs[ITERATION_CONVERGENCE_OUTPUT_INDEX]);
                executionLineageNode.addPredecessor(inputs[ITERATION_CONVERGENCE_INPUT_INDEX].getLineage());
                break;
            default:
                throw new IllegalStateException(String.format("%s is finished, yet executed.", this));

        }

        if (endloop) {
            // final loop output
            JavaExecutionOperator.forward(input, outputs[FINAL_OUTPUT_INDEX]);
            outputs[ITERATION_OUTPUT_INDEX] = null;
            outputs[ITERATION_CONVERGENCE_OUTPUT_INDEX] = null;
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
        return "rheem.java.loop.load";
    }

    @Override
    public Optional<LoadProfileEstimator> createLoadProfileEstimator(Configuration configuration) {
        final Optional<LoadProfileEstimator> optEstimator =
                JavaExecutionOperator.super.createLoadProfileEstimator(configuration);
        LoadProfileEstimators.nestUdfEstimator(optEstimator, this.criterionDescriptor, configuration);
        return optEstimator;
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new JavaLoopOperator<>(this.getInputType(),
                this.getConvergenceType(),
                this.getCriterionDescriptor().getJavaImplementation(),
                this.getNumExpectedIterations()
        );
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        switch (index) {
            case INITIAL_INPUT_INDEX:
            case ITERATION_INPUT_INDEX:
                return Arrays.asList(CollectionChannel.DESCRIPTOR, StreamChannel.DESCRIPTOR);
            case INITIAL_CONVERGENCE_INPUT_INDEX:
            case ITERATION_CONVERGENCE_INPUT_INDEX:
                return Collections.singletonList(CollectionChannel.DESCRIPTOR);
            default:
                throw new IllegalStateException(String.format("%s has no %d-th input.", this, index));
        }
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        switch (index) {
            case ITERATION_OUTPUT_INDEX:
            case FINAL_OUTPUT_INDEX:
                return Collections.singletonList(StreamChannel.DESCRIPTOR);
            case INITIAL_CONVERGENCE_INPUT_INDEX:
            case ITERATION_CONVERGENCE_INPUT_INDEX:
                return Collections.singletonList(CollectionChannel.DESCRIPTOR);
            default:
                throw new IllegalStateException(String.format("%s has no %d-th input.", this, index));
        }        // TODO: In this specific case, the actual output Channel is context-sensitive because we could forward Streams/Collections.
    }


}
