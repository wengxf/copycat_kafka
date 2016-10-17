package org.apache.kafka.common.protocol.types;

import java.nio.ByteBuffer;

import org.apache.kafka.common.utils.Utils;

public abstract class Type {

	public abstract void write(ByteBuffer buffer, Object o);
	
	public abstract Object read(ByteBuffer buffer);
	
	public abstract Object validate(Object o);
	
	public abstract int sizeOf(Object o);
	
	public boolean isNullable(){
		return false;
	}
	
	public static final Type BOOLEAN = new Type() {
		
		 @Override
	        public void write(ByteBuffer buffer, Object o) {
	            if ((Boolean) o)
	                buffer.put((byte) 1);
	            else
	                buffer.put((byte) 0);
	        }

	        @Override
	        public Object read(ByteBuffer buffer) {
	            byte value = buffer.get();
	            return value != 0;
	        }

	        @Override
	        public int sizeOf(Object o) {
	            return 1;
	        }

	        @Override
	        public String toString() {
	            return "BOOLEAN";
	        }

	        @Override
	        public Boolean validate(Object item) {
	            if (item instanceof Boolean)
	                return (Boolean) item;
	            else
	                throw new SchemaException(item + " is not a Boolean.");
	        }
	};
	
	public static final Type INT8 = new Type() {
        @Override
        public void write(ByteBuffer buffer, Object o) {
            buffer.put((Byte) o);
        }

        @Override
        public Object read(ByteBuffer buffer) {
            return buffer.get();
        }

        @Override
        public int sizeOf(Object o) {
            return 1;
        }

        @Override
        public String toString() {
            return "INT8";
        }

        @Override
        public Byte validate(Object item) {
            if (item instanceof Byte)
                return (Byte) item;
            else
                throw new SchemaException(item + " is not a Byte.");
        }
    };

    public static final Type INT16 = new Type() {
        @Override
        public void write(ByteBuffer buffer, Object o) {
            buffer.putShort((Short) o);
        }

        @Override
        public Object read(ByteBuffer buffer) {
            return buffer.getShort();
        }

        @Override
        public int sizeOf(Object o) {
            return 2;
        }

        @Override
        public String toString() {
            return "INT16";
        }

        @Override
        public Short validate(Object item) {
            if (item instanceof Short)
                return (Short) item;
            else
                throw new SchemaException(item + " is not a Short.");
        }
    };

    public static final Type INT32 = new Type() {
        @Override
        public void write(ByteBuffer buffer, Object o) {
            buffer.putInt((Integer) o);
        }

        @Override
        public Object read(ByteBuffer buffer) {
            return buffer.getInt();
        }

        @Override
        public int sizeOf(Object o) {
            return 4;
        }

        @Override
        public String toString() {
            return "INT32";
        }

        @Override
        public Integer validate(Object item) {
            if (item instanceof Integer)
                return (Integer) item;
            else
                throw new SchemaException(item + " is not an Integer.");
        }
    };

    public static final Type INT64 = new Type() {
        @Override
        public void write(ByteBuffer buffer, Object o) {
            buffer.putLong((Long) o);
        }

        @Override
        public Object read(ByteBuffer buffer) {
            return buffer.getLong();
        }

        @Override
        public int sizeOf(Object o) {
            return 8;
        }

        @Override
        public String toString() {
            return "INT64";
        }

        @Override
        public Long validate(Object item) {
            if (item instanceof Long)
                return (Long) item;
            else
                throw new SchemaException(item + " is not a Long.");
        }
    };

    public static final Type STRING = new Type() {
        @Override
        public void write(ByteBuffer buffer, Object o) {
            byte[] bytes = Utils.utf8((String) o);
            if (bytes.length > Short.MAX_VALUE)
                throw new SchemaException("String length " + bytes.length + " is larger than the maximum string length.");
            buffer.putShort((short) bytes.length);
            buffer.put(bytes);
        }

        @Override
        public Object read(ByteBuffer buffer) {
            short length = buffer.getShort();
            if (length < 0)
                throw new SchemaException("String length " + length + " cannot be negative");
            if (length > buffer.remaining())
                throw new SchemaException("Error reading string of length " + length + ", only " + buffer.remaining() + " bytes available");

            byte[] bytes = new byte[length];
            buffer.get(bytes);
            return Utils.utf8(bytes);
        }

        @Override
        public int sizeOf(Object o) {
            return 2 + Utils.utf8Length((String) o);
        }

        @Override
        public String toString() {
            return "STRING";
        }

        @Override
        public String validate(Object item) {
            if (item instanceof String)
                return (String) item;
            else
                throw new SchemaException(item + " is not a String.");
        }
    };

    public static final Type NULLABLE_STRING = new Type() {
        @Override
        public boolean isNullable() {
            return true;
        }

        @Override
        public void write(ByteBuffer buffer, Object o) {
            if (o == null) {
                buffer.putShort((short) -1);
                return;
            }

            byte[] bytes = Utils.utf8((String) o);
            if (bytes.length > Short.MAX_VALUE)
                throw new SchemaException("String length " + bytes.length + " is larger than the maximum string length.");
            buffer.putShort((short) bytes.length);
            buffer.put(bytes);
        }

        @Override
        public Object read(ByteBuffer buffer) {
            short length = buffer.getShort();
            if (length < 0)
                return null;
            if (length > buffer.remaining())
                throw new SchemaException("Error reading string of length " + length + ", only " + buffer.remaining() + " bytes available");

            byte[] bytes = new byte[length];
            buffer.get(bytes);
            return Utils.utf8(bytes);
        }

        @Override
        public int sizeOf(Object o) {
            if (o == null)
                return 2;

            return 2 + Utils.utf8Length((String) o);
        }

        @Override
        public String toString() {
            return "NULLABLE_STRING";
        }

        @Override
        public String validate(Object item) {
            if (item == null)
                return null;

            if (item instanceof String)
                return (String) item;
            else
                throw new SchemaException(item + " is not a String.");
        }
    };

    public static final Type BYTES = new Type() {
        @Override
        public void write(ByteBuffer buffer, Object o) {
            ByteBuffer arg = (ByteBuffer) o;
            int pos = arg.position();
            buffer.putInt(arg.remaining());
            buffer.put(arg);
            arg.position(pos);
        }

        @Override
        public Object read(ByteBuffer buffer) {
            int size = buffer.getInt();
            if (size < 0)
                throw new SchemaException("Bytes size " + size + " cannot be negative");
            if (size > buffer.remaining())
                throw new SchemaException("Error reading bytes of size " + size + ", only " + buffer.remaining() + " bytes available");

            ByteBuffer val = buffer.slice();
            val.limit(size);
            buffer.position(buffer.position() + size);
            return val;
        }

        @Override
        public int sizeOf(Object o) {
            ByteBuffer buffer = (ByteBuffer) o;
            return 4 + buffer.remaining();
        }

        @Override
        public String toString() {
            return "BYTES";
        }

        @Override
        public ByteBuffer validate(Object item) {
            if (item instanceof ByteBuffer)
                return (ByteBuffer) item;
            else
                throw new SchemaException(item + " is not a java.nio.ByteBuffer.");
        }
    };

    public static final Type NULLABLE_BYTES = new Type() {
        @Override
        public boolean isNullable() {
            return true;
        }

        @Override
        public void write(ByteBuffer buffer, Object o) {
            if (o == null) {
                buffer.putInt(-1);
                return;
            }

            ByteBuffer arg = (ByteBuffer) o;
            int pos = arg.position();
            buffer.putInt(arg.remaining());
            buffer.put(arg);
            arg.position(pos);
        }

        @Override
        public Object read(ByteBuffer buffer) {
            int size = buffer.getInt();
            if (size < 0)
                return null;
            if (size > buffer.remaining())
                throw new SchemaException("Error reading bytes of size " + size + ", only " + buffer.remaining() + " bytes available");

            ByteBuffer val = buffer.slice();
            val.limit(size);
            buffer.position(buffer.position() + size);
            return val;
        }

        @Override
        public int sizeOf(Object o) {
            if (o == null)
                return 4;

            ByteBuffer buffer = (ByteBuffer) o;
            return 4 + buffer.remaining();
        }

        @Override
        public String toString() {
            return "NULLABLE_BYTES";
        }

        @Override
        public ByteBuffer validate(Object item) {
            if (item == null)
                return null;

            if (item instanceof ByteBuffer)
                return (ByteBuffer) item;

            throw new SchemaException(item + " is not a java.nio.ByteBuffer.");
        }
    };
}
