package com.mapr.drill;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.Float8Holder;

/**
 * A collection of random number samplers.
 */
public class RandomNumberGenerator {
    /**
     * Samples from a uniform random distribution.  Arguments include the min (inclusive) and max (exclusive) of the
     * resulting distribution.
     */
    @FunctionTemplate(isRandom = true, name = "random", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
    public static class Uniform implements DrillSimpleFunc {
        @Param
        Float8Holder low;
        @Param
        Float8Holder high;

        @Output
        Float8Holder output;

        public void setup() {
        }

        public void eval() {
            output.value = com.mapr.drill.RandomHelper.nextUniform(low.value, high.value);
        }
    }

    @FunctionTemplate(name = "rnorm", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
    public static class Normal implements DrillSimpleFunc {
        @Param
        Float8Holder mean;
        @Param
        Float8Holder std;

        @Output
        Float8Holder output;

        public void setup() {
        }

        public void eval() {
            output.value = com.mapr.drill.RandomHelper.nextNormal(mean.value, std.value);
        }
    }
}
