package cn.edu.thu.tsfile.avro;

import cn.edu.thu.tsfile.avro.common.Constant;
import cn.edu.thu.tsfile.avro.common.FieldNotFoundException;
import cn.edu.thu.tsfile.avro.common.FieldNotValidException;
import cn.edu.thu.tsfile.avro.common.SeriesConfig;
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
import java.util.*;

public class AvroConverter {
	
	private static final Logger logger = LoggerFactory.getLogger(AvroConverter.class);
	
	private ConverterUtil convertUtil = new ConverterUtil();
	private Map yamlAttributes = null;
	private Map<String, String> keyAlias = new HashMap<>();

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

	public JSONObject convertSchema(Schema avroSchema) throws Exception {

		if (!avroSchema.getType().equals(Schema.Type.RECORD)) {
			throw new IllegalArgumentException("Avro schema must be a record.");
		}
		logger.debug("The avro schema is: {}", avroSchema);

		JSONArray measureGroup = resolveFields(avroSchema.getFields());

		JSONObject tsfileSchema = new JSONObject();
		tsfileSchema.put(JsonFormatConstant.DELTA_TYPE, Constant.DELTA_TYPE);
		tsfileSchema.put(JsonFormatConstant.JSON_SCHEMA, measureGroup);

		logger.debug("the jsonobject converted from avro schema is: {}", tsfileSchema);
		return tsfileSchema;

	}


	public TSRecord convertRecord(GenericRecord avroRecord) throws FieldNotFoundException {

        Schema schema = avroRecord.getSchema();

        Long timestamp = Long.valueOf(avroRecord.get(Constant.TIMESTAMP).toString());
        if (timestamp == null) {
            throw new FieldNotFoundException(Constant.TIMESTAMP + " is not found");
        }

        String delta_object = null;
        for(String key: keyAlias.keySet()) {
        	String value = avroRecord.get(key).toString();
        	if(delta_object == null)
	        	delta_object = keyAlias.get(key) + Constant.DELTA_OBJECT_VALUE_SEPARATOR + value;
			else
				delta_object += Constant.DELTA_OBJECT_SEPARATOR + keyAlias.get(key) + Constant.DELTA_OBJECT_VALUE_SEPARATOR + value;
        }
        
        TSRecord tsRecord = new TSRecord(timestamp, delta_object);

        //each field is a DataPoint
        for (Schema.Field field : schema.getFields()) {
        	if(isKey(field) || field.name().equals(Constant.TIMESTAMP) || field.name().equals(Constant.DELTA_TYPE))
        		continue;

        	Object fieldData = avroRecord.get(field.name());

			if (fieldData != null) {
				TSDataType tsDataType = getDataType(field.schema());
				DataPoint dataPoint = DataPoint.getDataPoint(tsDataType, aliasOrName(field), fieldData.toString());
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
	
	private JSONArray resolveFields(List<Schema.Field> avroFields) throws FieldNotFoundException {
		
		boolean timestampFlag = false;

		JSONArray measureGroup = new JSONArray();

		for (Schema.Field field : avroFields) {
			if (field.name().equals(Constant.DELTA_TYPE))
				continue;
			if (field.name().equals(Constant.TIMESTAMP)) {
				timestampFlag = true;
			} else {
				try {
					JSONObject measurement = resolveField(field);
					if(measurement != null)
						measureGroup.put(measurement);
				} catch (FieldNotValidException | FieldNotFoundException e) {
					e.printStackTrace();
				}
			}
		}
		
		if (!timestampFlag) {
			throw new FieldNotFoundException(
					"the Timestamp field(" + Constant.TIMESTAMP + ") is not found !");
		} else {
			return measureGroup;
		}
	}
	
	private JSONObject resolveField(Schema.Field field) throws FieldNotValidException, FieldNotFoundException {

		if(isKey(field)) {
			keyAlias.put(field.name(), aliasOrName(field));
			return null;
		}

		SeriesConfig seriesConfig = new SeriesConfig();

		String measurementId = aliasOrName(field);
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

	private String aliasOrName(Schema.Field field) {
		return field.aliases().iterator().hasNext()?field.aliases().iterator().next():field.name();
	}

	private boolean isKey(Schema.Field field) {
		Map props = field.getJsonProps();
		return props != null && props.containsKey("iskey");
	}
}
