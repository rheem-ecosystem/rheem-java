package io.rheem.java.mapping;

import io.rheem.basic.operators.MapPartitionsOperator;
import io.rheem.core.mapping.Mapping;
import io.rheem.core.mapping.OperatorPattern;
import io.rheem.core.mapping.PlanTransformation;
import io.rheem.core.mapping.ReplacementSubplanFactory;
import io.rheem.core.mapping.SubplanPattern;
import io.rheem.core.types.DataSetType;
import io.rheem.java.operators.JavaMapPartitionsOperator;
import io.rheem.java.platform.JavaPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link MapPartitionsOperator} to {@link JavaMapPartitionsOperator}.
 */
@SuppressWarnings("unchecked")
public class MapPartitionsMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(
                this.createSubplanPattern(),
                this.createReplacementSubplanFactory(),
                JavaPlatform.getInstance()
        ));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "mappartitions", new MapPartitionsOperator<>(null, DataSetType.none(), DataSetType.none()), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }


    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<MapPartitionsOperator>(
                (matchedOperator, epoch) -> new JavaMapPartitionsOperator<>(matchedOperator).at(epoch)
        );
    }
}
