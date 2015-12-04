package com.mapr.drill;

import org.apache.drill.exec.expr.holders.RepeatedFloat8Holder;
import org.apache.drill.exec.expr.holders.RepeatedIntHolder;
import org.apache.drill.exec.vector.Float8Vector;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;

/**
 * Auxilliary functions that assist in zipping lists go here.  They can't go
 * in-line in the Zip function because of limitations on how the code generation
 * works in Drill.
 */
public class ZipWriter {
    public void writeInt(BaseWriter.ComplexWriter writer, RepeatedIntHolder ... in) {
        int n = -1;
        for (RepeatedIntHolder input : in) {
            if (n != -1 && n != input.end - input.start) {
                throw new IllegalArgumentException("Arguments to zip must all be same length");
            }
            n = input.end - input.start;
        }
        if (n == -1) {
            throw new IllegalArgumentException("Must have at least one argument to zip");
        }

        IntVector.Accessor[] v = new IntVector.Accessor[in.length];
        for (int i = 0; i < in.length; i++) {
            v[i] = in[i].vector.getAccessor();
        }

        writer.setValueCount(n);
        BaseWriter.ListWriter outer = writer.rootAsList();
        outer.startList(); // [ outer list

        for (int i = 0; i < n; i++) {
            BaseWriter.ListWriter inner = outer.list();
            inner.startList();
            for (IntVector.Accessor accessor : v) {
                inner.integer().writeInt(accessor.get(i));
            }
            inner.endList();   // ] inner list
        }

        outer.endList(); // ] outer list
    }

    public void writeFloat8(BaseWriter.ComplexWriter writer, RepeatedFloat8Holder... in) {
        int n = -1;
        for (RepeatedFloat8Holder input : in) {
            if (n != -1 && n != input.end - input.start) {
                throw new IllegalArgumentException("Arguments to zip must all be same length");
            }
            n = input.end - input.start;
        }
        if (n == -1) {
            throw new IllegalArgumentException("Must have at least one argument to zip");
        }

        Float8Vector.Accessor[] v = new Float8Vector.Accessor[in.length];
        for (int i = 0; i < in.length; i++) {
            v[i] = in[i].vector.getAccessor();
        }

        writer.setValueCount(n);
        BaseWriter.ListWriter outer = writer.rootAsList();
        outer.startList(); // [ outer list

        for (int i = 0; i < n; i++) {
            BaseWriter.ListWriter inner = outer.list();
            inner.startList();
            for (Float8Vector.Accessor accessor : v) {
                inner.float8().writeFloat8(accessor.get(i));
            }
            inner.endList();   // ] inner list
        }

        outer.endList(); // ] outer list
    }
}
