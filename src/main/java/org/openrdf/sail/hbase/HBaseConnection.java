package org.openrdf.sail.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.openrdf.OpenRDFUtil;
import org.openrdf.cursor.CollectionCursor;
import org.openrdf.cursor.Cursor;
import org.openrdf.model.Namespace;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.query.BindingSet;
import org.openrdf.query.algebra.QueryModel;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.evaluation.impl.BindingAssigner;
import org.openrdf.query.algebra.evaluation.impl.CompareOptimizer;
import org.openrdf.query.algebra.evaluation.impl.ConjunctiveConstraintSplitter;
import org.openrdf.query.algebra.evaluation.impl.ConstantOptimizer;
import org.openrdf.query.algebra.evaluation.impl.DisjunctiveConstraintOptimizer;
import org.openrdf.query.algebra.evaluation.impl.EvaluationStrategyImpl;
import org.openrdf.query.algebra.evaluation.impl.FilterOptimizer;
import org.openrdf.query.algebra.evaluation.impl.QueryModelPruner;
import org.openrdf.query.algebra.evaluation.impl.SameTermFilterOptimizer;
import org.openrdf.query.algebra.evaluation.util.QueryOptimizerList;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.query.impl.EmptyBindingSet;
import org.openrdf.sail.hbase.data.HBaseStatementCursor;
import org.openrdf.sail.hbase.data.TripleTable;
import org.openrdf.sail.hbase.data.ValueTable;
import org.openrdf.sail.helpers.DefaultSailChangedEvent;
import org.openrdf.sail.helpers.NotifyingSailConnectionBase;
import org.openrdf.sail.helpers.SailConnectionBase;
import org.openrdf.store.Isolation;
import org.openrdf.store.StoreException;

public class HBaseConnection extends SailConnectionBase {

	protected final HBaseStore store;
	
	protected HashMap<String, HTable> tables;
	private volatile DefaultSailChangedEvent sailChangedEvent;

	/*--------------*
	 * Constructors *
	 *--------------*/

	protected HBaseConnection(HBaseStore store) throws IOException {
		super();
		
		this.store = store;
		this.tables = new HashMap<String, HTable>(this.store.getTripleTable().getIndexes().length + 2);

		this.sailChangedEvent = new DefaultSailChangedEvent(store);

		// Set default isolation level (serializable since we don't allow
		// concurrent transactions yet)
		try {
			setTransactionIsolation(Isolation.SERIALIZABLE);
		} catch (StoreException e) {
			throw new RuntimeException("unexpected exception", e);
		}
	}

	/*---------*
	 * Methods *
	 *---------*/

	public ValueFactory getValueFactory() {
		return store.getValueFactory();
	}

	public Cursor<? extends BindingSet> evaluate(QueryModel query,
			BindingSet bindings, boolean includeInferred) throws StoreException {
		logger.trace("Incoming query model:\n{}", query);

		// Clone the tuple expression to allow for more aggressive optimizations
		query = query.clone();

		replaceValues(query);

		HBaseTripleSource tripleSource = new HBaseTripleSource(store, this,
				includeInferred, !isAutoCommit());
		EvaluationStrategyImpl strategy = new EvaluationStrategyImpl(
				tripleSource, query);

		QueryOptimizerList optimizerList = new QueryOptimizerList();
		optimizerList.add(new BindingAssigner());
		optimizerList.add(new ConstantOptimizer(strategy));
		optimizerList.add(new CompareOptimizer());
		optimizerList.add(new ConjunctiveConstraintSplitter());
		optimizerList.add(new DisjunctiveConstraintOptimizer());
		optimizerList.add(new SameTermFilterOptimizer());
		optimizerList.add(new QueryModelPruner());
		// optimizerList.add(new QueryJoinOptimizer(new NativeEvaluationStatistics(store)));
		optimizerList.add(new FilterOptimizer());

		optimizerList.optimize(query, bindings);
		logger.trace("Optimized query model:\n{}", query);

		Cursor<BindingSet> iter;
		iter = strategy.evaluate(query, EmptyBindingSet.getInstance());
		return iter;
	}

	protected void replaceValues(TupleExpr tupleExpr) throws StoreException {
		// Replace all Value objects stored in variables with HBaseValue
		// objects which cache internal IDs
		tupleExpr.visit(new QueryModelVisitorBase<StoreException>() {

			@Override
			public void meet(Var var) {
				if (var.hasValue()) {
					var.setValue(store.getValueStore().getHBaseValue(
							var.getValue()));
				}
			}
		});
	}

	public Cursor<? extends Resource> getContextIDs() throws StoreException {
		try {
			Cursor<? extends Resource> contextIter;
			contextIter = store.getContextIDs(this);

			return contextIter;
		} catch (IOException e) {
			throw new StoreException(e);
		}
	}

	public Cursor<? extends Statement> getStatements(Resource subj, URI pred,
			Value obj, boolean includeInferred, Resource... contexts)
			throws StoreException {
		try {
			Cursor<? extends Statement> iter;
			iter = store.createStatementCursor(this, subj, pred, obj,
					includeInferred, !isAutoCommit(), contexts);

			return iter;
		} catch (IOException e) {

			throw new StoreException("Unable to get statements", e);
		}
	}

	public long size(Resource subj, URI pred, Value obj,
			boolean includeInferred, Resource... contexts)
			throws StoreException {
		try {
			ValueStore valueStore = store.getValueStore();

			byte[] subjID = null;
			if (subj != null) {
				subjID = valueStore.getID(subj);
				if (subjID == null) {
					return 0;
				}
			}
			byte[] predID = null;
			if (pred != null) {
				predID = valueStore.getID(pred);
				if (predID == null) {
					return 0;
				}
			}
			byte[] objID = null;
			if (obj != null) {
				objID = valueStore.getID(obj);
				if (objID == null) {
					return 0;
				}
			}
			
			List<byte[]> contextIDs;
			if (contexts != null && contexts.length == 0) {
				contextIDs = Arrays.asList(new byte[][] { null });
			} else {
				contextIDs = store.getContextIDs(OpenRDFUtil.notNull(contexts));
			}

			long size = 0L;
			for (byte[] contextID : contextIDs) {
				size += store.getTripleTable().size(this, subjID, predID, objID, contextID);
			}

			return size;
		} catch (IOException e) {
			throw new StoreException(e);
		}
	}

	public Cursor<? extends Namespace> getNamespaces() throws StoreException {
		return new CollectionCursor<Namespace>(store.getNamespaceTable());
	}

	public String getNamespace(String prefix) throws StoreException {
		return store.getNamespaceTable().getNamespace(prefix);
	}

	protected void setAutoFlush(boolean autoFlush) {
		for (HTable table : this.tables.values())
			table.setAutoFlush(autoFlush);
	}

	public HTable getHTable(String tableName) {
		if (!this.tables.containsKey(tableName))
		{
			HTable table = this.store.getHTable(tableName);
			table.setAutoFlush(this.isAutoCommit());
			try {
				table.setWriteBufferSize(2 << 20);
			}
			catch (IOException ioe) {
				logger.error("Error setting the write buffer size", ioe);
			}
						
			this.tables.put(tableName, table);
		}

		return this.tables.get(tableName);
	}

	public void putHTable(HTable table) {
		String tableName = Bytes.toString(table.getTableName());
		if (this.tables.containsKey(tableName))
			this.tables.remove(tableName);

		this.store.putHTable(table);
	}

	@Override
	public void begin() throws StoreException {
		super.begin();

		/*
		if (this.puts == null)
			this.puts = new HashMap<String, ArrayList<Put>>(this.store
					.getTripleTable().getIndexes().length);

		if (this.deletes == null)
			this.deletes = new HashMap<String, ArrayList<Delete>>(this.store
					.getTripleTable().getIndexes().length);
		*/
		
		for (HTable table : this.tables.values())
			table.setAutoFlush(false);
	}

	@Override
	public void commit() throws StoreException {

		if (this.isAutoCommit())
			throw new StoreException("Transaction not started; cannot commit.");

		try {
			//for (Map.Entry<String, ArrayList<Put>> p : this.puts.entrySet())
			//	this.getHTable(p.getKey()).put(p.getValue());
			//for (Map.Entry<String, ArrayList<Delete>> d : this.deletes.entrySet())
			//	this.getHTable(d.getKey()).delete(d.getValue());

			for (HTable table : this.tables.values()) {
				table.flushCommits();
				table.setAutoFlush(true);
			}

			super.commit();

			//store.notifySailChanged(sailChangedEvent);

			// create a fresh event object.
			sailChangedEvent = new DefaultSailChangedEvent(store);
		} catch (IOException ioe) {
			throw new StoreException(ioe);
		} catch (RuntimeException e) {
			logger.error(
					"Encountered an unexpected problem while trying to commit",
					e);
			throw e;
		}
	}

	@Override
	public void rollback() throws StoreException {
		if (this.isAutoCommit())
			throw new StoreException("Transaction not started; cannot commit.");

		try {
			//for (ArrayList<Put> p : this.puts.values())
			//	p.clear();

			//for (ArrayList<Delete> d : this.deletes.values())
			//	d.clear();

			super.rollback();

		} catch (RuntimeException e) {
			logger
					.error(
							"Encountered an unexpected problem while trying to roll back",
							e);
			throw e;
		}
	}
/*
	public void addPut(String tableName, Put put) {
		
		if (!this.puts.containsKey(tableName))
			this.puts.put(tableName, new ArrayList<Put>());

		this.puts.get(tableName).add(put);
	}

	public void addDelete(String tableName, Delete del) {
		if (!this.deletes.containsKey(tableName))
			this.deletes.put(tableName, new ArrayList<Delete>());

		this.deletes.get(tableName).add(del);
	}
*/
	public void addStatement(Resource subj, URI pred, Value obj, Resource... contexts) throws StoreException {
		addStatement(subj, pred, obj, true, contexts);
	}

	public void addInferredStatement(Resource subj, URI pred, Value obj,	Resource... contexts) throws StoreException {
		addStatement(subj, pred, obj, false, contexts);
	}

	private static int triplesAdded = 0;
	private void addStatement(final Resource subj, final URI pred, final Value obj, final boolean explicit, Resource... contexts) throws StoreException {

		if (contexts != null && contexts.length == 0) {
			contexts = new Resource[] { null };
		}
		
		final Resource[] adjustedContexts = contexts;
		
		/*
		this.store.execute(
			new Runnable() {

				@Override
				public void run() {
		*/
					//boolean result = false;

					try {
						ValueStore values = store.getValueStore();
						byte[] subjID = values.storeValue(subj);
						byte[] predID = values.storeValue(pred);
						byte[] objID = values.storeValue(obj);
					
						for (Resource context : OpenRDFUtil.notNull(adjustedContexts)) {
			
							byte[] contextID = context != null ? values.storeValue(context) : ValueTable.NULL_CONTEXT;
							boolean wasNew = store.getTripleTable().storeTriple(HBaseConnection.this, subjID, predID, objID, contextID, explicit);
							//result |= wasNew;
							
							if (++triplesAdded % 10000 == 0)
								logger.debug("This connection has added " + triplesAdded + " triples.");
			
							if (wasNew) {
								// The triple was not yet present in the triple store
								sailChangedEvent.setStatementsAdded(true);
								/*
								if (hasConnectionListeners()) {
									Statement st;
			
									if (context != null) {
										st = values.createStatement(subj, pred, obj,
												context);
									} else {
										st = values.createStatement(subj, pred, obj);
									}
			
									notifyStatementAdded(st);
									
								}
								*/
							}
						}
					} catch (IOException ioe) {
						logger.error("Encountered an unexpected problem while trying to add a statement.", ioe);
					} catch (RuntimeException re) {
						logger.error("Encountered an unexpected problem while trying to add a statement.", re);
					}
			//	}
			//});

		//return result;
	}

	public void removeStatements(Resource subj, URI pred, Value obj,
			Resource... contexts) throws StoreException {
		removeStatements(subj, pred, obj, true, contexts);
	}

	public boolean removeInferredStatements(Resource subj, URI pred, Value obj,
			Resource... contexts) throws StoreException {
		int removeCount = removeStatements(subj, pred, obj, false, contexts);
		return removeCount > 0;
	}

	private int removeStatements(Resource subj, URI pred, Value obj,
			boolean explicit, Resource... contexts) throws StoreException {
		try {
			TripleTable triples = store.getTripleTable();
			ValueStore values = store.getValueStore();

			byte[] subjID = null;
			if (subj != null) {
				subjID = values.getID(subj);
				if (subjID == null) {
					return 0;
				}
			}
			byte[] predID = null;
			if (pred != null) {
				predID = values.getID(pred);
				if (predID == null) {
					return 0;
				}
			}
			byte[] objID = null;
			if (obj != null) {
				objID = values.getID(obj);
				if (objID == null) {
					return 0;
				}
			}

			contexts = OpenRDFUtil.notNull(contexts);
			List<byte[]> contextIDList = new ArrayList<byte[]>(contexts.length);
			if (contexts.length == 0) {
				contextIDList.add(null);
			} else {
				for (Resource context : contexts) {
					if (context == null) {
						contextIDList.add(null);
					} else {
						byte[] contextID = values.getID(context);
						if (contextID != null) {
							contextIDList.add(contextID);
						}
					}
				}
			}

			int removeCount = 0;

			for (int i = 0; i < contextIDList.size(); i++) {
				byte[] contextID = contextIDList.get(i);

				List<Statement> removedStatements = Collections.emptyList();

				/*
				if (hasConnectionListeners()) {
					// We need to iterate over all matching triples so that they
					// can be reported
					HBaseStatementCursor iter = triples.getTriples(this,
							subjID, predID, objID, contextID, explicit, true);

					removedStatements = new ArrayList<Statement>();
					Statement st;
					while ((st = iter.next()) != null) {
						removedStatements.add(st);
					}
				}
				 */
				removeCount += triples.removeTriples(this, subjID, predID,
						objID, contextID, explicit);
				/*
				for (Statement st : removedStatements) {
					notifyStatementRemoved(st);
				}
				*/
			}

			if (removeCount > 0) {
				sailChangedEvent.setStatementsRemoved(true);
			}

			return removeCount;
		} catch (IOException e) {
			throw new StoreException(e);
		} catch (RuntimeException e) {
			logger
					.error(
							"Encountered an unexpected problem while trying to remove statements",
							e);
			throw e;
		}
	}

	public void setNamespace(String prefix, String name) throws StoreException {
		try {
			store.getNamespaceTable().setNamespace(prefix, name);
		} catch (IOException ioe) {
			throw new HBaseException(ioe);
		}
	}

	public void removeNamespace(String prefix) throws StoreException {
		try {
			store.getNamespaceTable().removeNamespace(prefix);
		} catch (IOException ioe) {
			throw new HBaseException(ioe);
		}
	}

	public void clearNamespaces() throws StoreException {
		try {
			this.store.getNamespaceTable().clear();
		} catch (IOException e) {
			throw new HBaseException(e);
		}
	}
	
	@Override
	public void close() throws StoreException {

		try {
			for (HTable table : this.tables.values()) {
				if (!table.isAutoFlush())
					table.flushCommits();

				this.store.putHTable(table);
			}
		} catch (IOException ioe) {
			throw new HBaseException(ioe);
		}

		super.close();
	}
}
