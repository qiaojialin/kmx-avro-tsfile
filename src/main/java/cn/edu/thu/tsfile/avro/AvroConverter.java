package cn.edu.thu.tsfile.avro;

import cn.edu.thu.tsfile.common.constant.JsonFormatConstant;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.timeseries.write.record.DataPoint;
import cn.edu.thu.tsfile.timeseries.write.record.TSRecord;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class AvroConverter {
	
	private static final Logger logger = LoggerFactory.getLogger(AvroConverter.class);
	
	private ConverterUtil convertUtil = new ConverterUtil();
	private Map yamlAttributes = null;

	public AvroConverter() {

	}

	public AvroConverter(String confPath) throws FileNotFoundException {
		if(confPath != null && !confPath.equals(""))
			yamlAttributes = convertUtil.loadYaml(confPath);
	}

	public AvroConverter(String confPath, Configuration conf) throws IOException {
		if(confPath != null && !confPath.equals(""))
			yamlAttributes = convertUtil.loadYaml(confPath, conf);
	}
	
	public TSRecord convertRecord(GenericRecord avroRecord) throws FieldNotFoundException {

        Schema schema = avroRecord.getSchema();

        Long timestamp = Long.valueOf(avroRecord.get(Constant.TIMESTAMP).toString());
        if (timestamp == null) {
            throw new FieldNotFoundException(Constant.TIMESTAMP + " is not found");
        }

        String delta_object = avroRecord.get(Constant.DELTA_OBJECT).toString();
        if (delta_object == null) {
            throw new FieldNotFoundException(Constant.DELTA_OBJECT + " is not found");
        }
        
        TSRecord tsRecord = new TSRecord(timestamp, delta_object);

        //each field is a DataPoint
        for (Schema.Field field : schema.getFields()) {
        	
        	String fieldName = field.name();
        	Object fieldData = avroRecord.get(fieldName);

			if (!fieldName.equals(Constant.TIMESTAMP)
					&& !fieldName.equals(Constant.DELTA_OBJECT) && fieldData != null) {
				TSDataType tsDataType = getDataType(field.schema());
				DataPoint dataPoint = DataPoint.getDataPoint(tsDataType, fieldName, fieldData.toString());
				tsRecord.addTuple(dataPoint);
			}
		}
        return tsRecord;
    }
	
	private TSDataType getDataType(Schema fieldSchema) throws FieldNotFoundException {

		Schema.Type type = fieldSchema.getType();
    	
    	if (type == null) {
			throw new FieldNotFoundException("Type is not found");
		}

		if (type.equals(Schema.Type.UNION)) {
			Schema.Type unionType = null;
    		for (Schema unionSchema : fieldSchema.getTypes()) {
    			if (!unionSchema.getType().equals(Schema.Type.NULL)) {
					if (unionType != null) {
						throw new UnsupportedOperationException(
								"Cannot convert Avro union of more than one type.");
					}
    				unionType = unionSchema.getType();
				}
    		}
            return convertUtil.convertAvroTypeToTsfile(unionType);
		} else {
			return convertUtil.convertAvroTypeToTsfile(type);
		}
	}

	public JSONObject convertSchema(Schema avroSchema) throws Exception {
		
		if (!avroSchema.getType().equals(Schema.Type.RECORD)) {
			throw new IllegalArgumentException("Avro schema must be a record.");
		}
		logger.debug("The avro schema is: {}", avroSchema.getFields());

		JSONArray measureGroup = resolveFields(avroSchema.getFields());

		JSONObject tsfileSchema = new JSONObject();
		tsfileSchema.put(JsonFormatConstant.DELTA_TYPE, Constant.DELTA_TYPE);
		tsfileSchema.put(JsonFormatConstant.JSON_SCHEMA, measureGroup);

		logger.debug("the jsonobject converted from avro schema is: {}", tsfileSchema);
		return tsfileSchema;
		
	}
	
	private JSONArray resolveFields(List<Schema.Field> avroFields) throws FieldNotFoundException {
		
		boolean timestampFlag = false;
		boolean deltaObjectFlag = false;

		JSONArray measureGroup = new JSONArray();

		for (Schema.Field field : avroFields) {
			
			if (field.name().equals(Constant.TIMESTAMP)) {
				timestampFlag = true;
			} else if (field.name().equals(Constant.DELTA_OBJECT)) {
				deltaObjectFlag = true;
			} else {
				try {
					JSONObject measurement = resolveField(field);
					measureGroup.put(measurement);
				} catch (FieldNotValidException | FieldNotFoundException e) {
					e.printStackTrace();			
				}
			}
		}
		
		if (!timestampFlag) {
			throw new FieldNotFoundException(
					"the Timestamp field(" + Constant.TIMESTAMP + ") is not found !");
		} else if (!deltaObjectFlag) {
			throw new FieldNotFoundException(
					"the DeviceId field(" + Constant.DELTA_OBJECT + ") is not found !");
		} else {
			return measureGroup;
		}
	}
	
	private JSONObject resolveField(Schema.Field field) throws FieldNotValidException, FieldNotFoundException {
		SeriesConfig seriesConfig = new SeriesConfig();

		String measurementId = field.name();
		seriesConfig.setMeasurementId(measurementId);
		seriesConfig.setSeriesType(field.schema());

		if(yamlAttributes == null)
			return seriesConfig.getSensorConfigs();

		if (yamlAttributes.containsKey(measurementId)) {

			Map sensorAttrs = (Map)yamlAttributes.get(measurementId);
			for (Object sensorAttr : sensorAttrs.keySet()) {
				if (sensorAttr.toString().equals(Constant.SENSOR_ENCODING)) {
					seriesConfig.setSensorEncoding((String)sensorAttrs.get(sensorAttr));
				} else {
					seriesConfig.addOtherAttribute(sensorAttr, sensorAttrs.get(sensorAttr));
				}
			}
		}
		return seriesConfig.getSensorConfigs();
	}
}
