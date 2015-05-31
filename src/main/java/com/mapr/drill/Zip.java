package com.mapr.drill;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.RepeatedFloat8Holder;
import org.apache.drill.exec.expr.holders.RepeatedIntHolder;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;


/**
 * Replicates the python zip function, but only for pairs of lists containing numbers.
 *
 * If you have data like this:
 * <pre>
 * +------+--------------------+--------------------+--------------------+
 * |  n   |         t          |         x          |         y          |
 * +------+--------------------+--------------------+--------------------+
 * | 1.0  | [1.0,2.0,3.0]      | [3.0,2.0,1.0]      | [4.0,5.0,6.0]      |
 * | 2.0  | [0.0,1.0,2.0,3.0]  | [0.0,3.0,2.0,1.0]  | [7.0,6.0,5.0,4.0]  |
 * +------+--------------------+--------------------+--------------------+
 * </pre>
 * Then a query like this
 * <pre>
 *   select n, data[0] as t, data[1] as v from (select n, flatten( zip(t,x)) as data from dfs.root.`/Users/tdunning/tmp/data.json`);
 * </pre>
 * Will produce these results
 * <pre>
 * +------+------+------+
 * |  n   |  t   |  v   |
 * +------+------+------+
 * | 1.0  | 1.0  | 3.0  |
 * | 1.0  | 2.0  | 2.0  |
 * | 1.0  | 3.0  | 1.0  |
 * | 2.0  | 1.0  | 3.0  |
 * | 2.0  | 2.0  | 2.0  |
 * | 2.0  | 3.0  | 1.0  |
 * | 2.0  | 0.0  | 0.0  |
 * +------+------+------+
 * </pre>
 * Two, three or four arguments can be used.  Thus,
 * <pre>
 *   select n, data[0] as t, data[1] as x, data[2] as y from (select n, flatten( zip(t,x,y)) as data from dfs.root.`/Users/tdunning/tmp/data.json`);
 * </pre>
 * will produce
 * <pre>
 * +------+------+------+------+
 * |  n   |  t   |  x   |  y   |
 * +------+------+------+------+
 * | 1.0  | 1.0  | 3.0  | 4.0  |
 * | 1.0  | 2.0  | 2.0  | 5.0  |
 * | 1.0  | 3.0  | 1.0  | 6.0  |
 * | 2.0  | 1.0  | 3.0  | 4.0  |
 * | 2.0  | 2.0  | 2.0  | 5.0  |
 * | 2.0  | 3.0  | 1.0  | 6.0  |
 * | 2.0  | 0.0  | 0.0  | 7.0  |
 * +------+------+------+------+
 * </pre>
 */
@SuppressWarnings("unused")
public class Zip {
    @FunctionTemplate(name = "zip", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
    public static class IntZip2 implements DrillSimpleFunc {
        @Param
        RepeatedIntHolder in1;
        @Param
        RepeatedIntHolder in2;

        @Output
        BaseWriter.ComplexWriter writer;

        public void setup() {
        }

        public void eval() {
            new com.mapr.drill.ZipWriter().writeInt(writer, in1, in2);
        }
    }

    @FunctionTemplate(name = "zip", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
    public static class IntZip3 implements DrillSimpleFunc {
        @Param
        RepeatedIntHolder in1;
        @Param
        RepeatedIntHolder in2;
        @Param
        RepeatedIntHolder in3;

        @Output
        BaseWriter.ComplexWriter writer;

        public void setup() {
        }

        public void eval() {
            new com.mapr.drill.ZipWriter().writeInt(writer, in1, in2, in3);
        }
    }

    @FunctionTemplate(name = "zip", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
    public static class IntZip4 implements DrillSimpleFunc {
        @Param
        RepeatedIntHolder in1;
        @Param
        RepeatedIntHolder in2;
        @Param
        RepeatedIntHolder in3;
        @Param
        RepeatedIntHolder in4;

        @Output
        BaseWriter.ComplexWriter writer;

        public void setup() {
        }

        public void eval() {
            new com.mapr.drill.ZipWriter().writeInt(writer, in1, in2, in3, in4);
        }
    }

    @FunctionTemplate(name = "zip", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
    public static class FloatZip2 implements DrillSimpleFunc {
        @Param
        RepeatedFloat8Holder in1;
        @Param
        RepeatedFloat8Holder in2;

        @Output
        BaseWriter.ComplexWriter writer;

        public void setup() {
        }

        @Override
        protected Object clone() throws CloneNotSupportedException {
            return super.clone();
        }

        public void eval() {
            new com.mapr.drill.ZipWriter().writeFloat8(writer, in1, in2);
        }
    }

    @FunctionTemplate(name = "zip", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
    public static class FloatZip3 implements DrillSimpleFunc {
        @Param
        RepeatedFloat8Holder in1;
        @Param
        RepeatedFloat8Holder in2;
        @Param
        RepeatedFloat8Holder in3;

        @Output
        BaseWriter.ComplexWriter writer;

        public void setup() {
        }

        @Override
        protected Object clone() throws CloneNotSupportedException {
            return super.clone();
        }

        public void eval() {
            new com.mapr.drill.ZipWriter().writeFloat8(writer, in1, in2, in3);
        }
    }

    @FunctionTemplate(name = "zip", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
    public static class FloatZip4 implements DrillSimpleFunc {
        @Param
        RepeatedFloat8Holder in1;
        @Param
        RepeatedFloat8Holder in2;
        @Param
        RepeatedFloat8Holder in3;
        @Param
        RepeatedFloat8Holder in4;

        @Output
        BaseWriter.ComplexWriter writer;

        public void setup() {
        }

        @Override
        protected Object clone() throws CloneNotSupportedException {
            return super.clone();
        }

        public void eval() {
            new com.mapr.drill.ZipWriter().writeFloat8(writer, in1, in2, in3, in4);
        }
    }
}
