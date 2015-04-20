package com.kresilas.hll;


import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class HashUDFTest {

    @Test
    public void initialize() throws UDFArgumentException {
        final ObjectInspector[] goodObjectInspectors = {
                PrimitiveObjectInspectorFactory.javaStringObjectInspector
        };

        final HashUDF hashUDF = new HashUDF();
        assertEquals(
                PrimitiveObjectInspectorFactory.javaLongObjectInspector,
                hashUDF.initialize(goodObjectInspectors));
    }

    @Test(expected = UDFArgumentException.class)
    public void initializeBadType() throws UDFArgumentException {
        final ObjectInspector[] badTypeObjectInspectors = {
                PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector
        };

        final HashUDF hashUDF = new HashUDF();
        hashUDF.initialize(badTypeObjectInspectors);
    }

    @Test(expected = UDFArgumentException.class)
    public void initializeBadSize() throws UDFArgumentException {
        final ObjectInspector[] badSizeObjectInspectors = {
                PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                PrimitiveObjectInspectorFactory.javaStringObjectInspector
        };
        final HashUDF hashUDF = new HashUDF();
        hashUDF.initialize(badSizeObjectInspectors);
    }

    @Test
    public void evaluate() throws HiveException {
        final String input = "asdf";
        final Long output = 1168293687029170440L;

        final GenericUDF.DeferredObject[] deferredObjects = {
                new GenericUDF.DeferredJavaObject(input)
        };

        final ObjectInspector[] objectInspectors = {
                PrimitiveObjectInspectorFactory.javaStringObjectInspector
        };

        final HashUDF hashUDF = new HashUDF();
        hashUDF.initialize(objectInspectors);

        assertEquals(hashUDF.evaluate(deferredObjects), output);
    }
}
