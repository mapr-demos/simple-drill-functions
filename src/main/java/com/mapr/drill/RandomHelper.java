package com.mapr.drill;

import org.apache.drill.exec.expr.holders.Float8Holder;

import java.util.Random;

/**
 * Created by tdunning on 7/20/15.
 */
public class RandomHelper {
    private static Random gen = new Random();

    public static double nextUniform(double low, double high) {
        return gen.nextDouble() * (high - low) + low;
    }

    public static double nextNormal(double mean, double std) {
        return gen.nextGaussian() * std + mean;
    }
}
