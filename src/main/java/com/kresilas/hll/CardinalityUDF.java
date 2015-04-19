package com.kresilas.hll;

import net.agkn.hll.HLL;
import net.agkn.hll.util.NumberUtil;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

/**
 * Get the cardinality of an HLL
 */
@Description(name = "hll_cardinality",
    value = "_FUNC_(x) estimate cardinality of HLL")
public class CardinalityUDF extends GenericUDF {

    private StringObjectInspector stringObjectInspector;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length != 1 || objectInspectors[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            // TODO better error about number of parameters
            throw new UDFArgumentException("hll_cardinality takes a binary object");
        }

        final PrimitiveObjectInspector primitiveObjectInspector = (PrimitiveObjectInspector) objectInspectors[0];
        if (primitiveObjectInspector.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentException("hll_cardinality takes a string object");
        }

        this.stringObjectInspector = (StringObjectInspector) primitiveObjectInspector;

        // return Type
        return PrimitiveObjectInspectorFactory.javaLongObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        final String input = stringObjectInspector.getPrimitiveJavaObject((Object) deferredObjects[0].get());

        if (input == null) {
            return null;
        }

        try {
            final byte[] buffer = NumberUtil.fromHex(input, 2, input.length() - 2);
            final HLL hll = HLL.fromBytes(buffer);
            return hll.cardinality();
        } catch (Exception e) {
            throw new HiveException(e);
        }
    }

    @Override
    public String getDisplayString(String[] arg0) {
        StringBuilder sb = new StringBuilder("hll_cardinality( ");
        for (int i = 0; i < arg0.length - 1; ++i) {
            sb.append(arg0[i]);
            sb.append(" , ");
        }
        sb.append(arg0[arg0.length - 1]);
        sb.append(" )");
        return sb.toString();
    }
}
