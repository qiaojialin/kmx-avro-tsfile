package cn.edu.thu.tsfile.avro;

import cn.edu.thu.tsfile.avro.common.Constant;
import cn.edu.thu.tsfile.common.constant.JsonFormatConstant;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.thu.tsfile.timeseries.write.record.DataPoint;
import cn.edu.thu.tsfile.timeseries.write.record.TSRecord;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;


/**
 *
 * @author stefanie
 * Example avro to tsfile converter
 */
public class AvroConverterTest {

    private Schema avroSchema;
    private JSONObject tsfileSchema;
    private AvroConverter converter = new AvroConverter();

    @Before
    public void before() throws Exception {

        List<Schema.Field> avroFields = new ArrayList<>();
        avroFields.add(new Schema.Field(Constant.TIMESTAMP, Schema.create(Schema.Type.LONG), null, null));
        Schema.Field key1 = new Schema.Field("key1", Schema.create(Schema.Type.STRING), null, null);

        key1.addProp("iskey", "true");
        avroFields.add(key1);
        avroFields.add(new Schema.Field("s1", getUnionSchema(Schema.Type.INT), null, null));
        avroFields.add(new Schema.Field("s2", getUnionSchema(Schema.Type.LONG), null, null));
        avroFields.add(new Schema.Field("s3", getUnionSchema(Schema.Type.FLOAT), null, null));
        avroFields.add(new Schema.Field("s4", getUnionSchema(Schema.Type.DOUBLE), null, null));
        avroFields.add(new Schema.Field("s5", getUnionSchema(Schema.Type.BOOLEAN), null, null));

        avroSchema = Schema.createRecord("fg_3000014", null, "k2data", false);
        avroSchema.addAlias("windfarm_BMdxz_001");
        avroSchema.setFields(avroFields);
        tsfileSchema = converter.convertSchema(avroSchema);
    }

    private Schema getUnionSchema(Schema.Type type) {
        List<Schema> unionTypes = new ArrayList<>();
        unionTypes.add(Schema.create(Schema.Type.NULL));
        unionTypes.add(Schema.create(type));
        Schema unionSchema = Schema.createUnion(unionTypes);
        return unionSchema;
    }

    @Test
    public void testConvertRecord() throws Exception {
        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put(Constant.TIMESTAMP, 1481117408000L);
        avroRecord.put("key1", "d1");
        avroRecord.put("s1", 1);
        avroRecord.put("s2", 2L);
        avroRecord.put("s3", Float.valueOf("1.3"));
        avroRecord.put("s4", Double.valueOf("1.4"));
        avroRecord.put("s5", true);


        TSRecord actualRecord = converter.convertRecord(avroRecord);


        Long time1 = 1481117408000L;
        String deviceId1 = "key1:d1";
        TSRecord expectedRecord = new TSRecord(time1, deviceId1);
        DataPoint dataPoint1 = DataPoint.getDataPoint(TSDataType.INT32, "s1", "1");
        DataPoint dataPoint2 = DataPoint.getDataPoint(TSDataType.INT64, "s2", "2");
        DataPoint dataPoint3 = DataPoint.getDataPoint(TSDataType.FLOAT, "s3", "1.3");
        DataPoint dataPoint4 = DataPoint.getDataPoint(TSDataType.DOUBLE, "s4", "1.4");
        DataPoint dataPoint5 = DataPoint.getDataPoint(TSDataType.BOOLEAN, "s5", "true");
        expectedRecord.addTuple(dataPoint1);
        expectedRecord.addTuple(dataPoint2);
        expectedRecord.addTuple(dataPoint3);
        expectedRecord.addTuple(dataPoint4);
        expectedRecord.addTuple(dataPoint5);
        System.out.println(expectedRecord.toString());

        Assert.assertEquals(expectedRecord.toString(), actualRecord.toString());
    }

    @Test
    public void testConvertSchema() throws Exception {
        JSONObject s1 = new JSONObject();
        s1.put(JsonFormatConstant.MEASUREMENT_UID, "s1");
        s1.put(JsonFormatConstant.DATA_TYPE, TSDataType.INT32.toString());
        s1.put(JsonFormatConstant.MEASUREMENT_ENCODING, TSEncoding.RLE.toString());

        JSONObject s2 = new JSONObject();
        s2.put(JsonFormatConstant.MEASUREMENT_UID, "s2");
        s2.put(JsonFormatConstant.DATA_TYPE, TSDataType.INT64.toString());
        s2.put(JsonFormatConstant.MEASUREMENT_ENCODING, TSEncoding.RLE.toString());

        JSONObject s3 = new JSONObject();
        s3.put(JsonFormatConstant.MEASUREMENT_UID, "s3");
        s3.put(JsonFormatConstant.DATA_TYPE, TSDataType.FLOAT.toString());
        s3.put(JsonFormatConstant.MEASUREMENT_ENCODING, TSEncoding.RLE.toString());

        JSONObject s4 = new JSONObject();
        s4.put(JsonFormatConstant.MEASUREMENT_UID, "s4");
        s4.put(JsonFormatConstant.DATA_TYPE, TSDataType.DOUBLE.toString());
        s4.put(JsonFormatConstant.MEASUREMENT_ENCODING, TSEncoding.RLE.toString());

        JSONObject s5 = new JSONObject();
        s5.put(JsonFormatConstant.MEASUREMENT_UID, "s5");
        s5.put(JsonFormatConstant.DATA_TYPE, TSDataType.BOOLEAN.toString());
        s5.put(JsonFormatConstant.MEASUREMENT_ENCODING, TSEncoding.RLE.toString());

        JSONArray measureGroup = new JSONArray();
        measureGroup.put(s1);
        measureGroup.put(s2);
        measureGroup.put(s3);
        measureGroup.put(s4);
        measureGroup.put(s5);

        JSONObject expectedSchema = new JSONObject();
        expectedSchema.put(JsonFormatConstant.DELTA_TYPE, "windfarm_bmdxz_001");
        expectedSchema.put(JsonFormatConstant.JSON_SCHEMA, measureGroup);

        Assert.assertEquals(expectedSchema.toString(), tsfileSchema.toString());
    }

}