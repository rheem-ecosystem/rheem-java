package io.rheem.java.mapping;

import io.rheem.basic.operators.MaterializedGroupByOperator;
import io.rheem.core.mapping.Mapping;
import io.rheem.core.mapping.OperatorPattern;
import io.rheem.core.mapping.PlanTransformation;
import io.rheem.core.mapping.ReplacementSubplanFactory;
import io.rheem.core.mapping.SubplanPattern;
import io.rheem.core.types.DataSetType;
import io.rheem.java.operators.JavaMaterializedGroupByOperator;
import io.rheem.java.platform.JavaPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link MaterializedGroupByOperator} to {@link JavaMaterializedGroupByOperator}.
 */
public class MaterializedGroupByMapping implements Mapping {

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
                "operator", new MaterializedGroupByOperator<>(null, DataSetType.none(), DataSetType.groupedNone()), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<MaterializedGroupByOperator<?, ?>>(
                (matchedOperator, epoch) -> new JavaMaterializedGroupByOperator<>(matchedOperator).at(epoch)
        );
    }
}
