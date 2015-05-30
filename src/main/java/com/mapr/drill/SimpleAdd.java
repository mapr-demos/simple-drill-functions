package com.mapr.drill;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.Float8Holder;
import org.apache.drill.exec.expr.holders.IntHolder;

/**
 * Created by tdunning on 5/30/15.
 */
public class SimpleAdd {
    @FunctionTemplate(name = "myaddints", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
    public static class IntIntAdd implements DrillSimpleFunc {
        @Param
        Float8Holder in1;
        @Param
        Float8Holder in2;
        @Output
        Float8Holder out;

        public void setup() {
        }

        public void eval() {
            out.value = in1.value + in2.value;
        }
    }
}
