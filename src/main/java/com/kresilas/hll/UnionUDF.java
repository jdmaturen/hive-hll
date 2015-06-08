package com.kresilas.hll;

import net.agkn.hll.util.NumberUtil;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.util.ArrayList;
import java.util.List;

/**
 * Union HLL columns together
 */
@Description(name = "hll_union",
        value = "_FUNC_(x, x) union hlls")
public class UnionUDF extends GenericUDF {
    private List<StringObjectInspector> inputStringOI;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        this.inputStringOI = new ArrayList<StringObjectInspector>(objectInspectors.length);

        for (final ObjectInspector objectInspector : objectInspectors) {
            if (objectInspector.getCategory() != ObjectInspector.Category.PRIMITIVE) {
                throw new UDFArgumentException("hll_union takes string inputs");
            }

            final PrimitiveObjectInspector primitiveObjectInspector = (PrimitiveObjectInspector) objectInspector;
            if (primitiveObjectInspector.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
                throw new UDFArgumentException("hll_union takes string inputs");
            }

            this.inputStringOI.add((StringObjectInspector) primitiveObjectInspector);
        }
        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        final HLLBuffer hllBuffer = new HLLBuffer();
        int i = 0;
        for (final DeferredObject deferredObject : deferredObjects) {
            final StringObjectInspector inputOI = inputStringOI.get(i);
            final String input = inputOI.getPrimitiveJavaObject(deferredObject.get());

            if (input == null) {
                continue;
            }

            try {
                final byte[] buffer = NumberUtil.fromHex(input, 2, input.length() - 2);
                hllBuffer.merge(buffer);
            } catch (Exception e) {
                throw new HiveException(e);
            }
        }

        return hllBuffer.toHex();
    }

    @Override
    public String getDisplayString(String[] arg0) {
        StringBuilder sb = new StringBuilder("hll_union( ");
        for (int i = 0; i < arg0.length - 1; ++i) {
            sb.append(arg0[i]);
            sb.append(" , ");
        }
        sb.append(arg0[arg0.length - 1]);
        sb.append(" )");
        return sb.toString();
    }
}
