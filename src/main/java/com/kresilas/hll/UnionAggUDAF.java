package com.kresilas.hll;

import net.agkn.hll.util.NumberUtil;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.log4j.Logger;

/**
* Aggregate function to union HLLs together
*/
@Description(name = "hll_union_agg",
    value = "_FUNC_(hll) union hlls")
public class UnionAggUDAF extends AbstractGenericUDAFResolver {

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        if (parameters.length != 1) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "Please specify one argument.");
        }

        if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0,
                    "Only primitive type arguments are accepted but "
                            + parameters[0].getTypeName()
                            + " was passed as parameter 1.");
        }

        if (((PrimitiveTypeInfo) parameters[0]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentTypeException(0,
                    "Only a string argument is accepted as parameter 1, but "
                            + parameters[0].getTypeName()
                            + " was passed instead.");
        }

        return new UnionHLLUDAFEvaluator();
    }

    public static class UnionHLLUDAFEvaluator extends GenericUDAFEvaluator {
        private static Logger log = Logger.getLogger(UnionHLLUDAFEvaluator.class);

        // for PARTIAL1 and COMPLETE
        private StringObjectInspector inputStringOI;

        // for PARTIAL2 and FINAL
        private BinaryObjectInspector partialBufferOI;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);

            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                this.inputStringOI = (StringObjectInspector) parameters[0];
                return PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
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
            ((HLLBuffer) aggregationBuffer).reset();
        }

        @Override
        public void iterate(AggregationBuffer aggregationBuffer, Object[] parameters) throws HiveException {
            if (parameters[0] == null) {
                return;
            }

            final String otherString = inputStringOI.getPrimitiveJavaObject(parameters[0]);
            try {
                final byte[] otherBytes = NumberUtil.fromHex(otherString, 2, otherString.length() - 2);
                ((HLLBuffer) aggregationBuffer).merge(otherBytes);
            } catch (Exception e) {
                throw new HiveException(e);
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer aggregationBuffer) throws HiveException {
            return ((HLLBuffer) aggregationBuffer).toBytes();
        }

        @Override
        public void merge(AggregationBuffer aggregationBuffer, Object partial) throws HiveException {
            if (partial == null) {
                return;
            }

            final byte[] partialBuffer = partialBufferOI.getPrimitiveJavaObject(partial);
            ((HLLBuffer) aggregationBuffer).merge(partialBuffer);
        }

        @Override
        public Object terminate(AggregationBuffer aggregationBuffer) throws HiveException {
            return ((HLLBuffer) aggregationBuffer).toHex();
        }
    }
}
