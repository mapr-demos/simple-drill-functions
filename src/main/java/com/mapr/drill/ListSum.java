package com.mapr.drill;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.Float8Holder;
import org.apache.drill.exec.expr.holders.RepeatedFloat8Holder;
import org.apache.drill.exec.vector.Float8Vector;

/**
 * Sum up the numbers in a list.
 *
 * If x.json has the following data
 *
 * {a:[1,2,3], b:[3,2,1]}
 * {a:[4,1,2,3], b:[0,3,2,1]}
 *
 * then this query
 *
 * select list_sum(a) from dfs.root.`/Users/tdunning/tmp/data.json`;
 *
 * Returns 6 and 10
 */
public class ListSum {
    @FunctionTemplate(name = "list_sum", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
    public static class Sum implements DrillSimpleFunc {
        @Param
        RepeatedFloat8Holder in;
        @Output
        Float8Holder out;

        public void setup() {
        }

        public void eval() {
            double r = 0;
            Float8Vector.Accessor accessor = in.vector.getAccessor();
            for (int i = in.start; i < in.end; i++) {
                r += accessor.get(i);
            }
            out.value = r;
        }
    }

}
