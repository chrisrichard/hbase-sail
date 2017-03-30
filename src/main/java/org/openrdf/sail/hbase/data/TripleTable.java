package org.openrdf.sail.hbase.data;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.openrdf.cursor.ConvertingCursor;
import org.openrdf.cursor.Cursor;
import org.openrdf.model.Resource;
import org.openrdf.query.algebra.evaluation.cursors.DistinctCursor;
import org.openrdf.query.algebra.evaluation.cursors.ReducedCursor;
import org.openrdf.sail.hbase.HBaseConnection;
import org.openrdf.sail.hbase.HBaseStore;
import org.openrdf.sail.hbase.ValueStore;
import org.openrdf.sail.hbase.config.IndexSpec;
import org.openrdf.sail.hbase.data.filter.DistinctValueFilter;
import org.openrdf.sail.hbase.model.HBaseResource;
import org.openrdf.store.StoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TripleTable {
	private Logger logger = LoggerFactory.getLogger(TripleTable.class);

	private HBaseStore store;
	private TripleIndex[] indexes;

	public TripleTable(HBaseStore store) {
		this.store = store;
		
		List<IndexSpec> indexSpecs = store.getHBaseStoreConfig().getTripleIndexes();
		this.indexes = new TripleIndex[indexSpecs.size()];
		for (int i = 0; i < this.indexes.length; ++i) {
			this.indexes[i] = new TripleIndex(store, indexSpecs.get(i));
		}
	}

	protected TripleIndex getBestIndex(byte[] subj, byte[] pred, byte[] obj,
			byte[] context) {
		int bestScore = -1;
		List<TripleIndex> bestIndexes = new ArrayList<TripleIndex>(indexes.length);

		for (int i = 0; i < indexes.length; ++i) {
			int score = indexes[i].getPatternScore(subj, pred, obj, context);
			if (score > bestScore) {
				bestScore = score;
				bestIndexes.clear();
				bestIndexes.add(indexes[i]);
			} else if (score == bestScore) {
				bestIndexes.add(indexes[i]);
			}
		}
			
		if (bestIndexes.size() == 1) {
			return bestIndexes.get(0);
		}
		else {
			return bestIndexes.get(new Random().nextInt(bestIndexes.size()));
			//return bestIndexes.get(0);
		}
	}
	
	public TripleIndex[] getIndexes()
	{
		return this.indexes;
	}

	public long size(HBaseConnection conn, byte[] subjID, byte[] predID, byte[] objID, byte[] contextID) throws IOException, StoreException {

		TripleIndex index = this.getBestIndex(subjID, predID, objID, contextID);
		HBaseCursor cursor = this.getTriplesInternal(
				conn.getHTable(index.getTableName()), 
				index, subjID, predID, objID, contextID);
		
		long size = 0;
		
		while (cursor.hasNext()) {
			cursor.next();
			++size;
		}
		
		return size;
	}

	public Cursor<Resource> getAllContextResources(HBaseConnection conn) throws IOException {
		
		final TripleIndex index = this.getBestIndex(null, null, null, ValueTable.MAX_VALUE);
		
		byte[] valueMask;
		if (index.getFieldSeq()[0] == 'c') {
			valueMask = ValueTable.MAX_VALUE;
		}
		else {
			int position = 0;
			for (int i = 1; i < 4; ++i) {
				if (index.getFieldSeq()[i] == 'c') {
					position = i;
				}
			}
			
			if (position == 0)
				throw new RuntimeException();
			
			valueMask = new byte[(position + 1) * ValueTable.NUM_VALUE_BYTES];
			Bytes.putBytes(valueMask, position * ValueTable.NUM_VALUE_BYTES, 
					ValueTable.MAX_VALUE, 0, ValueTable.NUM_VALUE_BYTES);
		}

		HTable table = conn.getHTable(index.getTableName());
		byte[] startKey = index.getStartKey(null, null, null, ValueTable.FIRST_VALUE);
		byte[] stopKey = index.getEndKey(null, null, null, null);
		Filter filter = new DistinctValueFilter(valueMask);
		
		ResultScanner scanner = HBaseTable.scan(table, startKey, stopKey, null, null, filter);
		
		return new ReducedCursor<Resource>(
				new ConvertingCursor<KeyValue, Resource>(
						new HBaseCursor(scanner)) {
			@Override
			protected Resource convert(KeyValue kv)
					throws StoreException {
				
				Resource context;
				try {
					context = (Resource)store.getValueStore().getValue(index.getValueIDs(kv)[3]);
				}
				catch (IOException ioe) {
					throw new StoreException(ioe);
				}
				
				return context; 
			}
		});
	}

	public HBaseStatementCursor getTriples(HBaseConnection conn, byte[] subjID, byte[] predID,
			byte[] objID, byte[] contextID, boolean readTransaction)
			throws IOException {

		return this.getTriples(conn, subjID, predID, objID, contextID, 
				true, readTransaction);
	}

	public HBaseStatementCursor getTriples(HBaseConnection conn, 
			byte[] subjID, byte[] predID, byte[] objID, byte[] contextID, 
			boolean explicit, boolean readTransaction) throws IOException {

		TripleIndex index = this.getBestIndex(subjID, predID, objID, contextID);
		HBaseCursor cursor = this.getTriplesInternal(
				conn.getHTable(index.getTableName()), 
				index, subjID, predID, objID, contextID);
		return new HBaseStatementCursor(cursor, index, store.getValueStore());
	}

	private HBaseCursor getTriplesInternal(HTable table, TripleIndex index, byte[] subjID,
		byte[] predID, byte[] objID, byte[] contextID) throws IOException {
		byte[] startKey = index.getStartKey(subjID, predID, objID, contextID);
		byte[] stopKey = index.getEndKey(subjID, predID, objID, contextID);
		byte[][] families = index.getFamilies(subjID, predID, objID, contextID);
		byte[] qualifier = index.getQualifier(subjID, predID, objID, contextID);
		Filter filter = index.getFilter(subjID, predID, objID, contextID);
		
		if (Bytes.compareTo(startKey, stopKey) != 0)
		{
			ResultScanner scanner = HBaseTable.scan(table, startKey, stopKey, families, qualifier, filter);
			return new HBaseCursor(scanner);			
		}
		else
		{
			Result result = HBaseTable.get(table, startKey, families, qualifier);
			return new HBaseCursor(result);
		}
	}

	public boolean storeTriple(HBaseConnection conn, byte[] subj, byte[] pred, byte[] obj,
			byte[] context) throws IOException {
		return this.storeTriple(conn, subj, pred, obj, context, true);
	}

	public boolean storeTriple(HBaseConnection conn, 
			byte[] subj, byte[] pred, byte[] obj, byte[] context,
			boolean explicit) throws IOException {
		for (TripleIndex index : indexes) {
			byte[] key = index.getKey(subj, pred, obj, context);
			byte[] family = index.getFamily(subj, pred, obj, context);
			byte[] qualifier = index.getQualifier(subj, pred, obj, context);
			byte[] value = index.getValue(subj, pred, obj, context);

			//if (conn.isAutoCommit())
			//{
				HBaseTable.put(conn.getHTable(index.getTableName()), key, family, qualifier, value);
			//}
			//else
			//{
			//	conn.addPut(index.getTableName(), HBaseTable.getPut(key, family, qualifier, value));
			//}
		}

		return true;
	}

	public int removeTriples(HBaseConnection conn, byte[] subj, byte[] pred, byte[] obj, byte[] ctx,
			boolean explicit) throws IOException {

		TripleIndex readIndex = this.getBestIndex(subj, pred, obj, ctx);
		HBaseCursor triples = this.getTriplesInternal(conn.getHTable(readIndex.getTableName()),
				readIndex, subj, pred,	obj, ctx);
		
		int count = 0;
		try {
			while (triples.hasNext()) {
				KeyValue kv = triples.next();
				byte[][] valueIDs = readIndex.getValueIDs(kv);

				for (int i = 0; i < this.indexes.length; ++i) {
					TripleIndex index = this.indexes[i];
					byte[] key = index.getKey(valueIDs[0], valueIDs[1],
							valueIDs[2], valueIDs[3]);
					byte[] family = index.getFamily(valueIDs[0], valueIDs[1],
							valueIDs[2], valueIDs[3]);
					byte[] qualifier = index.getQualifier(valueIDs[0],
							valueIDs[1], valueIDs[2], valueIDs[3]);

					//if (conn.isAutoCommit())
					//{
						HBaseTable.delete(conn.getHTable(index.getTableName()), key, family, qualifier);
					//}
					//else
					//{
					//	conn.addDelete(index.getTableName(), HBaseTable.getDelete(key, family, qualifier));
					//}
				}

				++count;
			}
		} catch (StoreException se) {
			throw new IOException(se);
		}

		return count;
	}
}
