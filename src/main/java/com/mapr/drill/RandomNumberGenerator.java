package com.mapr.drill;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.Float8Holder;

/**
 * Created by tdunning on 7/20/15.
 */
public class RandomNumberGenerator {
    @FunctionTemplate(name = "random", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
    public static class IntZip2 implements DrillSimpleFunc {
        @Param
        Float8Holder low;
        @Param
        Float8Holder high;

        @Output
        Float8Holder output;

        public void setup() {
        }

        public void eval() {
            output.value = com.mapr.drill.RandomHelper.nextUniform(low, high);
        }
    }
}
