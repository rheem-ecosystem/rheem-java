package org.qcri.rheem.java.execution;

import org.qcri.rheem.core.api.Job;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.function.ExtendedFunction;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.Executor;
import org.qcri.rheem.core.platform.PartialExecution;
import org.qcri.rheem.core.platform.PushExecutorTemplate;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.java.JavaPlatform;
import org.qcri.rheem.java.compiler.FunctionCompiler;
import org.qcri.rheem.java.operators.JavaExecutionOperator;

import java.util.Arrays;
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
    protected void open(ExecutionTask task, List<ChannelInstance> inputChannelInstances) {
        cast(task.getOperator()).open(toArray(inputChannelInstances), this.compiler);
    }

    @Override
    protected Tuple<List<ChannelInstance>, PartialExecution> execute(
            ExecutionTask task,
            List<ChannelInstance> inputChannelInstances,
            OptimizationContext.OperatorContext producerOperatorContext,
            boolean isForceExecution
    ) {
        // Provide the ChannelInstances for the output of the task.
        final ChannelInstance[] outputChannelInstances = this.createOutputChannelInstances(
                task, producerOperatorContext, inputChannelInstances
        );

        // Execute.
        long startTime = System.currentTimeMillis();
        try {
            cast(task.getOperator()).evaluate(toArray(inputChannelInstances), outputChannelInstances, this.compiler);
        } catch (Exception e) {
            throw new RheemException(String.format("Executing %s failed.", task), e);
        }
        long endTime = System.currentTimeMillis();
        long executionDuration = endTime - startTime;

        // Check how much we executed.
        PartialExecution partialExecution = this.handleLazyChannelLineage(
                task, inputChannelInstances, producerOperatorContext, outputChannelInstances, executionDuration
        );

        // Force execution if necessary.
        if (isForceExecution) {
            for (ChannelInstance outputChannelInstance : outputChannelInstances) {
                if (outputChannelInstance == null || !outputChannelInstance.getChannel().isReusable()) {
                    this.logger.warn("Execution of {} might not have been enforced properly. " +
                                    "This might break the execution or cause side-effects with the re-optimization.",
                            task);
                }
            }
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

    public static void openFunction(JavaExecutionOperator operator, Object function, ChannelInstance[] inputs) {
        if (function instanceof ExtendedFunction) {
            ExtendedFunction extendedFunction = (ExtendedFunction) function;
            extendedFunction.open(new JavaExecutionContext(operator, inputs));
        }
    }
}
