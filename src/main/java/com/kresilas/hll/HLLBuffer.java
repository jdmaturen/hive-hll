package com.kresilas.hll;

import net.agkn.hll.HLL;
import net.agkn.hll.HLLType;
import net.agkn.hll.util.NumberUtil;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;

/**
 * HLLBuffer used by AddAgg and UnionAgg
 */
class HLLBuffer implements GenericUDAFEvaluator.AggregationBuffer {
    private HLL hll;
    private boolean initialized = false;

    public boolean isInitialized() {
        return initialized;
    }

    public void init(final int log2m, final int regwidth, final int expthresh, final boolean sparseon) {
        hll = new HLL(log2m, regwidth, expthresh, sparseon, HLLType.EMPTY);
        initialized = true;
    }

    public void reset() {
        hll = null;
        initialized = false;
    }

    public void addRaw(final long rawValue) {
        hll.addRaw(rawValue);
    }

    public void merge(byte[] buffer) {
        if (buffer == null) {
            return;
        }

        final HLL other = HLL.fromBytes(buffer);

        if (hll == null) {
            hll = other;
            initialized = true;
        } else {
            hll.union(other);
        }
    }

    public byte[] toBytes() {
        if (hll == null) {
            return null;
        }

        return hll.toBytes();
    }

    public String toHex() {
        if (hll == null) {
            return null;
        }

        final byte[] bytes = hll.toBytes();
        return "\\x" + NumberUtil.toHex(bytes, 0, bytes.length);
    }
}