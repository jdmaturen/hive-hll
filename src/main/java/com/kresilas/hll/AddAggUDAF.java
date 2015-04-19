package com.kresilas.hll;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.log4j.Logger;

/**
 * HyperLogLog UDAF
 */
@Description(name = "hll_add_agg",
    value = "_FUNC_(x, [log2m, regwidth, expthresh, sparseon]) Constructs HLL estimator")
public class AddAggUDAF extends AbstractGenericUDAFResolver {

    private static Logger log = Logger.getLogger(AddAggUDAF.class);

    // https://github.com/aggregateknowledge/postgresql-hll#defaults
    static final int LOG2M = 11;
    static final int REGWIDTH = 5;
    static final int EXPTHRESH = -1;
    static final boolean SPARSEON = true;

    @SuppressWarnings("deprecation")
    @Override
    public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info) throws SemanticException {
        final TypeInfo[] parameters = info.getParameters();

        // validate the first parameter, which is the expression to compute over
        if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0,
                    "Only primitive type arguments are accepted but "
                            + parameters[0].getTypeName() + " was passed as parameter 1.");
        }
        switch (((PrimitiveTypeInfo) parameters[0]).getPrimitiveCategory()) {
            case LONG:
                break;
            case BYTE:
            case SHORT:
            case INT:
            case FLOAT:
            case DOUBLE:
            case STRING:
            case BOOLEAN:
            default:
                throw new UDFArgumentTypeException(0,
                        "Only long type arguments are accepted but "
                                + parameters[0].getTypeName() + " was passed as parameter 1.");
        }

        // TODO HLL config params
        return new HLLUDAFEvaluator();
    }

    public static class HLLUDAFEvaluator extends GenericUDAFEvaluator {

        private static Logger log = Logger.getLogger(HLLUDAFEvaluator.class);

        // for PARTIAL1 and COMPLETE
        private LongObjectInspector inputLongOI;

        // for PARTIAL2 and FINAL
        private BinaryObjectInspector partialBufferOI;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);

            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                this.inputLongOI = (LongObjectInspector) parameters[0];
                return PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
                // TODO HLL configuration parameters
            }

            // in PARTIAL2 and FINAL return string representation
            this.partialBufferOI = (BinaryObjectInspector) parameters[0];
            return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            return new HLLBuffer();
        }

        @Override
        public void reset(AggregationBuffer aggregationBuffer) throws HiveException {
            HLLBuffer hllBuffer = (HLLBuffer) aggregationBuffer;
            hllBuffer.reset();
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            assert (parameters.length == 1);

            if (parameters[0] == null) {
                return;
            }

            final HLLBuffer hllBuffer = (HLLBuffer) agg;
            if (!hllBuffer.isInitialized()) {
                // TODO read parameters
                hllBuffer.init(LOG2M, REGWIDTH, EXPTHRESH, SPARSEON);
            }

            final long rawValue = PrimitiveObjectInspectorUtils.getLong(parameters[0], inputLongOI);
            hllBuffer.addRaw(rawValue);
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            return ((HLLBuffer) agg).toBytes();
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            if (partial == null) {
                return;
            }

            final byte[] partialBuffer = partialBufferOI.getPrimitiveJavaObject(partial);
            ((HLLBuffer) agg).merge(partialBuffer);
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            return ((HLLBuffer) agg).toHex();
        }
    }
}
