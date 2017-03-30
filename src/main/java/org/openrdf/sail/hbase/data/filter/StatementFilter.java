package org.openrdf.sail.hbase.data.filter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.openrdf.sail.hbase.ValueStore;
import org.openrdf.sail.hbase.data.ValueTable;

public class StatementFilter implements Filter {

	protected byte[][] filterFields;

	public StatementFilter() {
	}

	public StatementFilter(byte[][] filterFields) {
		this.filterFields = filterFields;
	}

	@Override
	public Filter.ReturnCode filterKeyValue(KeyValue kv) {

		int numKeyFields = kv.getRowLength() / ValueTable.NUM_VALUE_BYTES;

		for (int fieldNum = 0; fieldNum < 4; ++fieldNum) {
			if (filterFields[fieldNum] != null) {
				if (fieldNum < numKeyFields) {
					if (Bytes.compareTo(
							filterFields[fieldNum], 0, ValueTable.NUM_VALUE_BYTES, 
							kv.getBuffer(),	kv.getRowOffset() + (fieldNum * ValueTable.NUM_VALUE_BYTES), ValueTable.NUM_VALUE_BYTES)
						!= 0) {
						return Filter.ReturnCode.NEXT_ROW;
					}
				} else if (fieldNum < numKeyFields + 1) {
					if (Bytes.compareTo(filterFields[fieldNum], 0,
							ValueTable.NUM_VALUE_BYTES, kv.getBuffer(), kv
									.getQualifierOffset(),
							ValueTable.NUM_VALUE_BYTES) != 0) {
						return Filter.ReturnCode.SKIP;
					}
				} else if (fieldNum < numKeyFields + 2) {
					if (Bytes.compareTo(filterFields[fieldNum], 0,
							ValueTable.NUM_VALUE_BYTES, kv.getBuffer(), kv
									.getValueOffset(),
							ValueTable.NUM_VALUE_BYTES) != 0) {
						return Filter.ReturnCode.SKIP;
					}
				}
			}
		}

		return Filter.ReturnCode.INCLUDE;
	}

	@Override
	public boolean filterAllRemaining() {
		// TODO
		return false;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		filterFields = new byte[4][];
		byte[] nullValue = null;
		
		for (int fieldNum = 0; fieldNum < 4; ++fieldNum) {
			filterFields[fieldNum] = Bytes.readByteArray(in);
			if (fieldNum == 0)
			{
				nullValue = filterFields[0];
				filterFields[0] = null;
			}
			else if (Bytes.equals(filterFields[fieldNum], nullValue))
				filterFields[fieldNum] = null;
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		for (int fieldNum = 0; fieldNum < 4; ++fieldNum) {
			if (filterFields[fieldNum] != null)
				Bytes.writeByteArray(out, filterFields[fieldNum]);
			else
				Bytes.writeByteArray(out, ValueTable.NULL_VALUE);
		}
	}

	@Override
	public boolean filterRow() {
		return false;
	}

	@Override
	public boolean filterRowKey(byte[] arg0, int arg1, int arg2) {
		return false;
	}

	@Override
	public void reset() {
	}
}
