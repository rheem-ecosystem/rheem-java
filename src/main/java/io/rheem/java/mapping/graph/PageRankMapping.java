package io.rheem.java.mapping.graph;

import io.rheem.basic.operators.PageRankOperator;
import io.rheem.core.mapping.Mapping;
import io.rheem.core.mapping.OperatorPattern;
import io.rheem.core.mapping.PlanTransformation;
import io.rheem.core.mapping.ReplacementSubplanFactory;
import io.rheem.core.mapping.SubplanPattern;
import io.rheem.java.operators.graph.JavaPageRankOperator;
import io.rheem.java.platform.JavaPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link PageRankOperator} to {@link JavaPageRankOperator}.
 */
public class PageRankMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(
                this.createSubplanPattern(), this.createReplacementSubplanFactory(), JavaPlatform.getInstance()
        ));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern<>(
                "pageRank", new PageRankOperator(1), false
        );
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<PageRankOperator>(
                (matchedOperator, epoch) -> new JavaPageRankOperator(matchedOperator).at(epoch)
        );
    }
}
