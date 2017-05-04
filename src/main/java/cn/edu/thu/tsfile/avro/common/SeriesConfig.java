package cn.edu.thu.tsfile.avro.common;

import cn.edu.thu.tsfile.avro.ConverterUtil;
import cn.edu.thu.tsfile.avro.common.Constant;
import cn.edu.thu.tsfile.avro.common.FieldNotFoundException;
import cn.edu.thu.tsfile.avro.common.FieldNotValidException;
import cn.edu.thu.tsfile.common.constant.JsonFormatConstant;
import cn.edu.thu.tsfile.file.metadata.enums.TSEncoding;
import org.apache.avro.Schema;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * SeriesConfig is a configure class. Every variables is public and has default value.
 * 
 * @author Stefanie Zhao
 * @author Jialin Qiao
 */

public class SeriesConfig {
	
	/**
     * Here is the must-required attributes: sensor_id and sensor_type.
     * Sensor_id and sensor_type is from schema in avro file.
     * 
     */
    private String MEASUREMENT_UID = "";
    private String DATA_TYPE = "";
    
    /**
     * Here is the must-required attributes: sensor encoding type.
     * It can be loaded from yaml file, the default value is RLE.
     */
    private String MEASUREMENT_ENCODING = TSEncoding.RLE.toString();
    
    /**
     * Here is the option attributes list: other sensor attributes.
     * It can be loaded from yaml file, the default value is null.
     */
    private Map<Object, Object> OTHER_ATTRIBUTES = new HashMap();
    
    private ConverterUtil convertUitl = new ConverterUtil();
    
    public SeriesConfig() {
    	
    }
    
    public void setMeasurementId(String measurementName) {
    	MEASUREMENT_UID = measurementName;
	}
    
    public void setSeriesType(Schema measurementSchema) throws FieldNotFoundException {

		Schema.Type type = measurementSchema.getType();
    	
    	if (type == null) {
			throw new FieldNotFoundException("Type is not found: " + measurementSchema.getName());
		}

		if (type.equals(Schema.Type.UNION)) {
			Schema.Type unionType = null;
    		for (Schema unionSchema : measurementSchema.getTypes()) {
    			if (!unionSchema.getType().equals(Schema.Type.NULL)) {
					if (unionType != null) {
						throw new UnsupportedOperationException(
								"Cannot convert Avro union of more than one type.");
					}
    				unionType = unionSchema.getType();
				}
    		}
            DATA_TYPE = convertUitl.convertAvroTypeToTsfile(unionType).toString();
		} else if (type.equals(Schema.Type.ENUM)) {
			DATA_TYPE = convertUitl.convertAvroTypeToTsfile(type).toString();
			setEnumValue(measurementSchema);
		}
		else {
			DATA_TYPE = convertUitl.convertAvroTypeToTsfile(type).toString();
		}
	}
    
    public void addOtherAttribute(Object sensorAttr, Object object) {
    	
    	OTHER_ATTRIBUTES.put(sensorAttr, object);
    }
    
    public void setSensorEncoding(String sensor_encoding) throws FieldNotValidException {
    	
    	try {
    		MEASUREMENT_ENCODING = TSEncoding.valueOf(sensor_encoding).toString();
    	} catch (Exception e) {
    		throw new FieldNotValidException("field encoding not valid :" + sensor_encoding);
    	}
    	
	}
    
    public JSONObject getSensorConfigs() {
    	
    	/*
    	 * return all the attributes of a sensor in a jsonObject
    	 */
    	
    	JSONObject sensorConfigs = new JSONObject();
    	sensorConfigs.put(JsonFormatConstant.MEASUREMENT_UID, MEASUREMENT_UID);
    	sensorConfigs.put(JsonFormatConstant.DATA_TYPE, DATA_TYPE);
    	sensorConfigs.put(JsonFormatConstant.MEASUREMENT_ENCODING, MEASUREMENT_ENCODING);
    	
    	for (Object key : OTHER_ATTRIBUTES.keySet()) {
    		sensorConfigs.put((String)key, OTHER_ATTRIBUTES.get(key));
    	}
    	
    	return sensorConfigs;
    }
    
    private String resolveEnumValue(List<String> enumValue) {
    	//TO DO, WHEN ENUM VALUE'S FORMAT IS SURE
		return enumValue.toString();
	}
    
    private void setEnumValue(Schema sensorSchema) throws FieldNotFoundException {
    	
    	try {
    		List<String> enumList = sensorSchema.getEnumSymbols();
    		addOtherAttribute(Constant.ENUM_VALUES, resolveEnumValue(enumList));
    	} catch (Exception e) {
    		throw new FieldNotFoundException(
					"the Timestamp field(" + Constant.ENUM_VALUES + ") is not found !");
    	}
    }

}
