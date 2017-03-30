package org.openrdf.sail.hbase.data;

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.openrdf.cursor.Cursor;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.sail.hbase.ValueStore;
import org.openrdf.store.StoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseStatementCursor implements Cursor<Statement> {
	private Logger logger = LoggerFactory.getLogger(HBaseStatementCursor.class);

	private HBaseCursor internalCursor;
	private TripleIndex index;
	private ValueStore values;

	public HBaseStatementCursor(HBaseCursor cursor,
			TripleIndex index, ValueStore values) {

		this.internalCursor = cursor ;
		this.index = index;
		this.values = values;
	}

	public boolean hasNext() throws StoreException {

		return this.internalCursor.hasNext();
	}

	public Statement next() throws StoreException {
		if (hasNext()) {

			try {
				KeyValue keyValue = internalCursor.next();

				byte[][] valueIDs = index.getValueIDs(keyValue);

				Resource subj = (Resource) values.getValue(valueIDs[0]);
				URI pred = (URI) values.getValue(valueIDs[1]);
				Value obj = values.getValue(valueIDs[2]);

				Resource context = null;
				byte[] contextID = valueIDs[3];
				if (!Bytes.equals(contextID, ValueTable.NULL_VALUE)) {
					context = (Resource) values.getValue(contextID);
				}

				Statement statement = values.createStatement(subj, pred, obj,
					context);
				return statement;

			} catch (IOException ioe) {
				throw new StoreException(ioe);
			}
		} else {
			return null;
		}
	}

	@Override
	public void close() throws StoreException {

		internalCursor.close();
	}
}
