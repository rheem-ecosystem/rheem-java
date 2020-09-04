package io.rheem.java.execution;

import io.rheem.core.api.Job;
import io.rheem.core.api.exception.RheemException;
import io.rheem.core.function.ExtendedFunction;
import io.rheem.core.optimizer.OptimizationContext;
import io.rheem.core.plan.executionplan.ExecutionTask;
import io.rheem.core.plan.rheemplan.ExecutionOperator;
import io.rheem.core.platform.ChannelInstance;
import io.rheem.core.platform.Executor;
import io.rheem.core.platform.PartialExecution;
import io.rheem.core.platform.PushExecutorTemplate;
import io.rheem.core.platform.lineage.ExecutionLineageNode;
import io.rheem.core.util.Formats;
import io.rheem.core.util.Tuple;
import io.rheem.java.compiler.FunctionCompiler;
import io.rheem.java.operators.JavaExecutionOperator;
import io.rheem.java.platform.JavaPlatform;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * {@link Executor} implementation for the {@link JavaPlatform}.
 */
public class JavaExecutor extends PushExecutorTemplate {

    private final JavaPlatform platform;

    private final FunctionCompiler compiler;

    public JavaExecutor(JavaPlatform javaPlatform, Job job) {
        super(job);
        this.platform = javaPlatform;
        this.compiler = new FunctionCompiler(job.getConfiguration());
    }

    @Override
    public JavaPlatform getPlatform() {
        return this.platform;
    }

    @Override
    protected Tuple<List<ChannelInstance>, PartialExecution> execute(
            ExecutionTask task,
            List<ChannelInstance> inputChannelInstances,
            OptimizationContext.OperatorContext producerOperatorContext,
            boolean isRequestEagerExecution
    ) {
        // Provide the ChannelInstances for the output of the task.
        final ChannelInstance[] outputChannelInstances = task.getOperator().createOutputChannelInstances(
                this, task, producerOperatorContext, inputChannelInstances
        );

        // Execute.
        final Collection<ExecutionLineageNode> executionLineageNodes;
        final Collection<ChannelInstance> producedChannelInstances;
        // TODO: Use proper progress estimator.
        this.job.reportProgress(task.getOperator().getName(), 50);
        long startTime = System.currentTimeMillis();
        try {
            final Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> results =
                    cast(task.getOperator()).evaluate(
                            toArray(inputChannelInstances),
                            outputChannelInstances,
                            this,
                            producerOperatorContext
                    );
            //Thread.sleep(1000);
            executionLineageNodes = results.getField0();
            producedChannelInstances = results.getField1();
        } catch (Exception e) {
            throw new RheemException(String.format("Executing %s failed.", task), e);
        }
        long endTime = System.currentTimeMillis();
        long executionDuration = endTime - startTime;

        this.job.reportProgress(task.getOperator().getName(), 100);

        // Check how much we executed.
        PartialExecution partialExecution = this.createPartialExecution(executionLineageNodes, executionDuration);

        if (partialExecution == null && executionDuration > 10) {
            this.logger.warn("Execution of {} took suspiciously long ({}).", task, Formats.formatDuration(executionDuration));
        }

        // Collect any cardinality updates.
        this.registerMeasuredCardinalities(producedChannelInstances);

        // Warn if requested eager execution did not take place.
        if (isRequestEagerExecution && partialExecution == null) {
            this.logger.info("{} was not executed eagerly as requested.", task);
        }

        return new Tuple<>(Arrays.asList(outputChannelInstances), partialExecution);
    }


    private static JavaExecutionOperator cast(ExecutionOperator executionOperator) {
        return (JavaExecutionOperator) executionOperator;
    }

    private static ChannelInstance[] toArray(List<ChannelInstance> channelInstances) {
        final ChannelInstance[] array = new ChannelInstance[channelInstances.size()];
        return channelInstances.toArray(array);
    }

    /**
     * Utility function to open an {@link ExtendedFunction}.
     *
     * @param operator        the {@link JavaExecutionOperator} containing the function
     * @param function        the {@link ExtendedFunction}; if it is of a different type, nothing happens
     * @param inputs          the input {@link ChannelInstance}s for the {@code operator}
     * @param operatorContext context information for the {@code operator}
     */
    public static void openFunction(JavaExecutionOperator operator,
                                    Object function,
                                    ChannelInstance[] inputs,
                                    OptimizationContext.OperatorContext operatorContext) {
        if (function instanceof ExtendedFunction) {
            ExtendedFunction extendedFunction = (ExtendedFunction) function;
            int iterationNumber = operatorContext.getOptimizationContext().getIterationNumber();
            extendedFunction.open(new JavaExecutionContext(operator, inputs, iterationNumber));
        }
    }

    public FunctionCompiler getCompiler() {
        return this.compiler;
    }
}
