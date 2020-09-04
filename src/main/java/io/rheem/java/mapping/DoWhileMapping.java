package io.rheem.java.mapping;

import io.rheem.basic.operators.DoWhileOperator;
import io.rheem.basic.operators.LoopOperator;
import io.rheem.core.function.PredicateDescriptor;
import io.rheem.core.mapping.Mapping;
import io.rheem.core.mapping.OperatorPattern;
import io.rheem.core.mapping.PlanTransformation;
import io.rheem.core.mapping.ReplacementSubplanFactory;
import io.rheem.core.mapping.SubplanPattern;
import io.rheem.core.types.DataSetType;
import io.rheem.java.operators.JavaDoWhileOperator;
import io.rheem.java.operators.JavaLoopOperator;
import io.rheem.java.platform.JavaPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link LoopOperator} to {@link JavaLoopOperator}.
 */
public class DoWhileMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(
                new PlanTransformation(
                        this.createSubplanPattern(),
                        this.createReplacementSubplanFactory(),
                        JavaPlatform.getInstance()
                )
        );
    }

    @SuppressWarnings("unchecked")
    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "loop", new DoWhileOperator<>(DataSetType.none(), DataSetType.none(), (PredicateDescriptor) null, 1), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<DoWhileOperator<?, ?>>(
                (matchedOperator, epoch) -> new JavaDoWhileOperator<>(matchedOperator).at(epoch)
        );
    }
}
