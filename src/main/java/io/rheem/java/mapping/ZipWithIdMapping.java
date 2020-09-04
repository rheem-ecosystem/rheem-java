package io.rheem.java.mapping;

import io.rheem.basic.data.Tuple2;
import io.rheem.basic.operators.ZipWithIdOperator;
import io.rheem.core.function.ExecutionContext;
import io.rheem.core.function.FunctionDescriptor;
import io.rheem.core.function.TransformationDescriptor;
import io.rheem.core.mapping.Mapping;
import io.rheem.core.mapping.OperatorPattern;
import io.rheem.core.mapping.PlanTransformation;
import io.rheem.core.mapping.ReplacementSubplanFactory;
import io.rheem.core.mapping.SubplanPattern;
import io.rheem.core.types.DataSetType;
import io.rheem.java.operators.JavaMapOperator;
import io.rheem.java.platform.JavaPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link ZipWithIdMapping} to a subplan.
 */
@SuppressWarnings("unchecked")
public class ZipWithIdMapping implements Mapping {

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
                "zipwithid", new ZipWithIdOperator<>(DataSetType.none()), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<ZipWithIdOperator<Object>>(
                (matchedOperator, epoch) -> {
                    final DataSetType<Object> inputType = matchedOperator.getInputType();
                    final DataSetType<Tuple2<Long, Object>> outputType = matchedOperator.getOutputType();
                    return new JavaMapOperator<>(
                            inputType,
                            outputType,
                            new TransformationDescriptor<>(
                                    new FunctionDescriptor.ExtendedSerializableFunction<Object, Tuple2<Long, Object>>() {

                                        private long nextId;

                                        @Override
                                        public void open(ExecutionContext ctx) {
                                            this.nextId = 0L;
                                        }

                                        @Override
                                        public Tuple2<Long, Object> apply(Object o) {
                                            return new Tuple2<>(this.nextId++, o);
                                        }
                                    },
                                    inputType.getDataUnitType().toBasicDataUnitType(),
                                    outputType.getDataUnitType().toBasicDataUnitType()
                            )
                    ).at(epoch);
                }
        );
    }
}
