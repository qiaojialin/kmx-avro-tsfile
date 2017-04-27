package cn.edu.thu.tsfile.avro;

public class FieldNotValidException extends Exception {
 
	private static final long serialVersionUID = -407941739879274554L; 

	public FieldNotValidException(String message) {
        super(message);
    }
}
