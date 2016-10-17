package org.apache.kafka.common.protocol.types;

/**
 * 
 * @author baodekang
 *
 */
public class Field {

	public static final Object NO_DEFAULT = new Object();
	
	public int index;
	public String name;
	public Type type;
	public Object defaultValue;
	public String doc;
	public Schema schema;
	
	public Field(int index, String name, Type type, String doc, Object defaultValue, Schema schema) {
		this.index = index;
		this.name = name;
		this.type = type;
		this.defaultValue = defaultValue;
		this.doc = doc;
		this.schema = schema;
		
		if(defaultValue != NO_DEFAULT){
			
		}
	}
	
	public Field(int index, String name, Type type, String doc, Object defaultValue) {
        this(index, name, type, doc, defaultValue, null);
    }

    public Field(String name, Type type, String doc, Object defaultValue) {
        this(-1, name, type, doc, defaultValue);
    }

    public Field(String name, Type type, String doc) {
        this(name, type, doc, NO_DEFAULT);
    }

    public Field(String name, Type type) {
        this(name, type, "");
    }

    public Type type() {
        return type;
    }

    public Schema schema() {
        return schema;
    }
}
