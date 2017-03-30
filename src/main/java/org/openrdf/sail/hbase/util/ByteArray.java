package org.openrdf.sail.hbase.util;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;

/*
 *	ByteArray wraps java byte arrays (byte[]) to allow byte arrays to be used
 *  as keys in hash tables. This is required because the equals function on
 *	byte[] directly uses reference equality.
 *	
 *	This class also allows the trio of array, offset and length to be carried
 *  around as a single object.
 */
public final class ByteArray
{
	private static final int SIZE_OF_INT = Integer.SIZE >> 3;
	
	private byte[] array;
	private int    offset;
	private int    length;

	/**
		Create an instance of this class that wraps ths given array.
		This class does not make a copy of the array, it just saves
		the reference.
	*/
	public ByteArray(byte[] array, int offset, int length) {
		this.array = array;
		this.offset = offset;
		this.length = length;
	}

	public ByteArray(byte[] array) {
		this(array, 0, array.length);
	}

	public ByteArray()
	{
	}

	public void setBytes(byte[] array)
	{
		this.array = array;
		offset = 0;
		length = array.length;
	}	

	public void setBytes(byte[] array, int length)
	{
		this.array = array;
		this.offset = 0;
		this.length = length;
	}	

	public void setBytes(byte[] array, int offset, int length)
	{
		this.array = array;
		this.offset = offset;
		this.length = length;
	}	

	public boolean equals(Object other) {
		if (other instanceof ByteArray) {
			ByteArray ob = (ByteArray) other;
			return Bytes.compareTo(array, offset, length, ob.array, ob.offset, ob.length) == 0;
		}
		return false;
	}

	public int hashCode() {

		int hash = length;
		for (int i = 0; i < length; i += SIZE_OF_INT)
			hash = hash ^ Bytes.toInt(array, i);

		return hash;
	}

	public final byte[] getArray() {
		return array;
	}
	public final int getOffset() {
		return offset;
	}

	public final int getLength() {
		return length;
	}
	public final void setLength(int newLength) {
		length = newLength;
	}
	
	/**
	 * Read this object from a stream of stored objects.
	 *
	 * @param in read this.
	 *
	 * @exception IOException					thrown on error
	 */
	public void readExternal( ObjectInput in ) throws IOException
	{
		int len = length = in.readInt();
		offset = 0; 
		array = new byte[len];

		in.readFully(array, 0, len);
	}


	/**
	 * Write the byte array out w/o compression
	 *
	 * @param out write bytes here.
	 *
	 * @exception IOException		thrown on error
	 */
	public void writeExternal(ObjectOutput out) throws IOException
	{
		out.writeInt(length);
		out.write(array, offset, length);
	}
}
