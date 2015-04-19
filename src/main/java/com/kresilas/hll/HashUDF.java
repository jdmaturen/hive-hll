package com.kresilas.hll;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.nio.charset.Charset;

/**
 * Hash input with Murmur3
 */
@Description(name = "hll_hash",
        value = "_FUNC_(x) get long hash of input using Murmur3")
public class HashUDF extends GenericUDF {

    final HashFunction hf = Hashing.murmur3_128();
    private StringObjectInspector inputStringOI;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {

        if (objectInspectors.length != 1 || objectInspectors[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            // TODO better error about number of parameters
            throw new UDFArgumentException("hll_hash takes a string");
        }

        final PrimitiveObjectInspector primitiveObjectInspector = (PrimitiveObjectInspector) objectInspectors[0];
        if (primitiveObjectInspector.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentException("hll_hash takes a string");
        }

        this.inputStringOI = (StringObjectInspector) primitiveObjectInspector;

        // return Type
        return PrimitiveObjectInspectorFactory.javaLongObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        final String input = inputStringOI.getPrimitiveJavaObject((Object) deferredObjects[0].get());

        if (input == null) {
            return null;
        }

        return hf.newHasher().putString(input, Charset.defaultCharset()).hash().asLong();
    }

    @Override
    public String getDisplayString(String[] arg0) {
        StringBuilder sb = new StringBuilder("hll_hash( ");
        for (int i = 0; i < arg0.length - 1; ++i) {
            sb.append(arg0[i]);
            sb.append(" , ");
        }
        sb.append(arg0[arg0.length - 1]);
        sb.append(" )");
        return sb.toString();
    }
}
