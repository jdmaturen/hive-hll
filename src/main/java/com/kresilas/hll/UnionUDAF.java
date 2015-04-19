//package com.kresilas.hll;
//
//import org.apache.hadoop.hive.ql.exec.Description;
//import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
//import org.apache.hadoop.hive.ql.metadata.HiveException;
//import org.apache.hadoop.hive.ql.parse.SemanticException;
//import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
//import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
//import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
//import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
//import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
//import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
//import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
//
///**
// * Aggregate function to union HLLs together
// */
//@Description(name = "hll_union_agg",
//    value = "_FUNC_(hll) union hlls")
//public class UnionUDAF extends AbstractGenericUDAFResolver {
//
//    @Override
//    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
//        if (parameters.length != 1) {
//            throw new UDFArgumentTypeException(parameters.length - 1,
//                    "Please specify one argument.");
//        }
//
//        if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
//            throw new UDFArgumentTypeException(0,
//                    "Only primitive type arguments are accepted but "
//                            + parameters[0].getTypeName()
//                            + " was passed as parameter 1.");
//        }
//
//        if (((PrimitiveTypeInfo) parameters[0]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
//            throw new UDFArgumentTypeException(0,
//                    "Only a string argument is accepted as parameter 1, but "
//                            + parameters[0].getTypeName()
//                            + " was passed instead.");
//        }
//
//        return new UnionHLLUDAFEvaluator();
//    }
//
//    public static class UnionHLLUDAFEvaluator extends GenericUDAFEvaluator {
//        private StringObjectInspector inputAndPartial
//
//        @Override
//        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
//            return null;
//        }
//
//        @Override
//        public void reset(AggregationBuffer aggregationBuffer) throws HiveException {
//
//        }
//
//        @Override
//        public void iterate(AggregationBuffer aggregationBuffer, Object[] objects) throws HiveException {
//
//        }
//
//        @Override
//        public Object terminatePartial(AggregationBuffer aggregationBuffer) throws HiveException {
//            return null;
//        }
//
//        @Override
//        public void merge(AggregationBuffer aggregationBuffer, Object o) throws HiveException {
//
//        }
//
//        @Override
//        public Object terminate(AggregationBuffer aggregationBuffer) throws HiveException {
//            return null;
//        }
//    }
//}
