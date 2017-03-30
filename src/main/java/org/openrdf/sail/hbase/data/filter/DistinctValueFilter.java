package org.openrdf.sail.hbase.data.filter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.openrdf.sail.hbase.data.ValueTable;

public class DistinctValueFilter extends FirstKeyOnlyFilter {

	byte[] valueMask;
	byte[] previousValue;
	
	public DistinctValueFilter() {		
	}
	
	public DistinctValueFilter(byte[] valueMask) {
		this.valueMask = valueMask;
	}	

	@Override
	public ReturnCode filterKeyValue(KeyValue v) {
		ReturnCode code = super.filterKeyValue(v);
		
		if (code == ReturnCode.INCLUDE) {
			
			int valueLength = Math.min(valueMask.length, v.getRowLength());
			byte[] value = new byte[valueLength];
			for (int i = 0; i < valueLength; ++i) {
				value[i] = (byte)(valueMask[i] & v.getBuffer()[v.getRowOffset() + i]);
			}
			
			if (previousValue != null && Bytes.compareTo(value, previousValue) == 0) {
				return ReturnCode.NEXT_ROW;
			} else {
				previousValue = value;
			}
		}
		
		return code;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		
		valueMask = Bytes.readByteArray(in);		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		
		Bytes.writeByteArray(out, valueMask);		
	}
}
