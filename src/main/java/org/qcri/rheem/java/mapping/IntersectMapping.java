package org.qcri.rheem.java.mapping;

import org.qcri.rheem.basic.operators.IntersectOperator;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.platform.JavaPlatform;
import org.qcri.rheem.java.operators.JavaIntersectOperator;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link IntersectOperator} to {@link JavaIntersectOperator}.
 */
public class IntersectMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(
                this.createSubplanPattern(),
                this.createReplacementSubplanFactory(),
                JavaPlatform.getInstance()
        ));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern<>(
                "intersect", new IntersectOperator<>(DataSetType.none()), false
        );
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<IntersectOperator<?>>(
                (matchedOperator, epoch) -> new JavaIntersectOperator<>(matchedOperator).at(epoch)
        );
    }
}
