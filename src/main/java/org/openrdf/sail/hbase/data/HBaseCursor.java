package org.openrdf.sail.hbase.data;

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.openrdf.cursor.Cursor;
import org.openrdf.sail.hbase.HBaseException;

public class HBaseCursor implements Cursor<KeyValue> {
		
	private ResultScanner scanner;
	private HTable table;
	private HTablePool tablePool;
	
	private Result currentResult;
	private int keyValueIndex = -1;
	
	private boolean advanced = false;
	private boolean hasNext;

	public HBaseCursor(ResultScanner scanner)
	{
		this(scanner, null, null);
	}
	
	public HBaseCursor(ResultScanner scanner, HTable table, HTablePool tablePool) {
		
		this.scanner = scanner;
		this.table = table;
		this.tablePool = tablePool;
	}
	
	public HBaseCursor(Result result)
	{
		this.currentResult = result;
	}

	public void close() {
		if (this.scanner != null)
			this.scanner.close();
		
		if (this.table != null && this.tablePool != null)
			this.tablePool.putTable(this.table);
	}

	public boolean hasNext() throws HBaseException {
		
		if (advanced) {
			return hasNext;
		}

		try {
			advanced = true;
			
			if (scanner != null && (keyValueIndex < 0 || (keyValueIndex + 1) >= currentResult.raw().length)) {
				currentResult = scanner.next();
				keyValueIndex = -1;
			}
			
			if (currentResult != null && (keyValueIndex + 1) < currentResult.raw().length) {
				++keyValueIndex;
				hasNext = true;
			}
			else {
				hasNext = false;
			}
						
			return hasNext;
			
		} catch (IOException ioe) {
			throw new HBaseException(ioe);
		}
	}

	public KeyValue next() throws HBaseException {
		
		if (hasNext()) {
			advanced = false;
			return currentResult.raw()[keyValueIndex];			
		} else {
			return null;
		}
	}
}
