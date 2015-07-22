package com.mapr.drill;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.VarBinaryHolder;

/**
 * This is a helper UDF that converts bytes in big-endian format into an integer that
 * Drill can use.
 */
public class BinaryIntConverter {
    /**
     * Converts 4 byte big-endian values into integers.
     */
    @FunctionTemplate(isRandom = true, name = "byte2int", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
    public static class BigEndianInt implements DrillSimpleFunc {
        @Param
        VarBinaryHolder raw;

        @Output
        IntHolder output;

        public void setup() {
        }

        public void eval() {
            output.value = raw.buffer.getInt(0);
        }
    }
}
