package org.openrdf.sail.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.openrdf.OpenRDFUtil;
import org.openrdf.cursor.Cursor;
import org.openrdf.cursor.EmptyCursor;
import org.openrdf.model.LiteralFactory;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.URIFactory;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.query.algebra.evaluation.cursors.UnionCursor;
import org.openrdf.sail.NotifyingSailConnection;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.hbase.config.HBaseStoreConfig;
import org.openrdf.sail.hbase.data.HBaseStatementCursor;
import org.openrdf.sail.hbase.data.HBaseTableFactory;
import org.openrdf.sail.hbase.data.NamespaceTable;
import org.openrdf.sail.hbase.data.TripleTable;
import org.openrdf.sail.hbase.data.ValueTable;
import org.openrdf.sail.helpers.NotifyingSailBase;
import org.openrdf.sail.helpers.SailBase;
import org.openrdf.store.StoreException;

public class HBaseStore extends SailBase {

	private HBaseStoreConfig conf;

	private ValueStore values;
	private TripleTable triples;
	private NamespaceTable namespaces;

	private HBaseConfiguration hbaseConf;
	private HTablePool tablePool;
	
	private Thread[] threads;
	private BlockingQueue<Runnable> queue;
	private volatile boolean shutDown = false;
	
	public HBaseStore() throws StoreException {
		this(new HBaseStoreConfig());
	}

	public HBaseStore(HBaseStoreConfig storeConf) throws StoreException {
		super();

		this.conf = storeConf;

		try {
			this.hbaseConf = new HBaseConfiguration();
			this.tablePool = new HTablePool();

			HBaseTableFactory tableFactory = new HBaseTableFactory(this, this.conf,
					this.hbaseConf);
			
			this.namespaces = tableFactory.getNamespaceTable();
			this.values = new ValueStore(this, tableFactory.getValueTable());
			this.triples = tableFactory.getTripleTable();
		} catch (IOException ioe) {
			throw new StoreException(ioe);
		}
	}
	
	public HBaseStoreConfig getHBaseStoreConfig() {
		return conf;
	}
	
	public HBaseConfiguration getHBaseConfiguration() {
		return this.hbaseConf;
	}
	
	public HTable getHTable(String tableName)
	{
		return this.tablePool.getTable(tableName);
	}

	public void putHTable(HTable table)
	{
		this.tablePool.putTable(table);
	}	
	
	private void initializeWorkerThreads()
	{
		final BlockingQueue<Runnable> queue = new ArrayBlockingQueue<Runnable>(8); 
		this.queue = queue;
		
		this.threads = new Thread[4];
		for (int i = 0; i < 4; ++i) {
			this.threads[i] = new Thread(
				new Runnable() {
					@Override
					public void run() {
						while (!shutDown) {
							try {
								queue.take().run();
							}
							catch (InterruptedException ie) {
								logger.error("Thread: " + Thread.currentThread().getName() + "was interrupted while waiting to take a new task.");
							}
						}
					}					
				}, "Store Worker " + i);
			
			this.threads[i].start();
		}
	}

	@Override
	protected SailConnection getConnectionInternal()
			throws StoreException {
		HBaseConnection hbaseConnection;
		try {
			hbaseConnection = new HBaseConnection(this);
		} catch (IOException ioe) {
			throw new HBaseException(ioe);
		}
		return hbaseConnection;
	}

	@Override
	public void initialize() throws StoreException {
		this.initializeWorkerThreads();
	}

	@Override
	protected void shutDownInternal() throws StoreException {
		this.shutDown = true;
		
		for (int i = 0; i < this.threads.length; ++i) {
			try {
				this.queue.put(
					new Runnable() { 
						public void run() {}
					});
			}
			catch (InterruptedException ie) {
				logger.error("Thread: " + Thread.currentThread().getName() + "was interrupted while waiting to put end tasks.");
			}
		}
	}

	@Override
	public LiteralFactory getLiteralFactory() {
		return values;
	}

	@Override
	public URIFactory getURIFactory() {
		return values;
	}

	public ValueFactory getValueFactory() {
		return values;
	}	
	
	public ValueStore getValueStore() {
		return values;
	}

	public TripleTable getTripleTable() {
		return triples;
	}

	public NamespaceTable getNamespaceTable() {
		return namespaces;
	}
		
	public void execute(Runnable r)
	{
		try {
			queue.put(r);
		}
		catch (InterruptedException ie) {
			logger.error("Thread: " + Thread.currentThread().getName() + "was interrupted while waiting to put a new task.");
		}
	}
		
	protected List<byte[]> getContextIDs(Resource... contexts)
			throws IOException {
		assert contexts == null || contexts.length > 0 : "contexts must not be empty";

		// Filter duplicates
		LinkedHashSet<Resource> contextSet = new LinkedHashSet<Resource>();
		Collections.addAll(contextSet, OpenRDFUtil.notNull(contexts));

		// Fetch IDs, filtering unknown resources from the result
		List<byte[]> contextIDs = new ArrayList<byte[]>(contextSet.size());
		for (Resource context : contextSet) {
			if (context == null) {
				contextIDs.add(ValueTable.NULL_CONTEXT);
			} else {
				byte[] contextID = values.getID(context);
				if (contextID != null) {
					contextIDs.add(contextID);
				}
			}
		}

		return contextIDs;
	}

	protected Cursor<Resource> getContextIDs(HBaseConnection conn)
			throws IOException {
		return triples.getAllContextResources(conn);
	}

	/**
	 * Creates a statement cursor based on the supplied pattern.
	 * 
	 * @param subj
	 *            The subject of the pattern, or <tt>null</tt> to indicate a
	 *            wildcard.
	 * @param pred
	 *            The predicate of the pattern, or <tt>null</tt> to indicate a
	 *            wildcard.
	 * @param obj
	 *            The object of the pattern, or <tt>null</tt> to indicate a
	 *            wildcard.
	 * @param contexts
	 *            The context(s) of the pattern. Note that this parameter is a
	 *            vararg and as such is optional. If no contexts are supplied
	 *            the method operates on the entire repository.
	 * @return A Cursor that can be used to iterate over the statements that
	 *         match the specified pattern.
	 */
	protected Cursor<? extends Statement> createStatementCursor(HBaseConnection conn, 
			Resource subj, URI pred, Value obj, boolean includeInferred,
			boolean readTransaction, Resource... contexts) throws IOException {

		byte[] subjID = null;
		if (subj != null) {
			subjID = values.getID(subj);
			if (subjID == null) {
				return new EmptyCursor<Statement>();
			}
		}

		byte[] predID = null;
		if (pred != null) {
			predID = values.getID(pred);
			if (predID == null) {
				return new EmptyCursor<Statement>();
			}
		}

		byte[] objID = null;
		if (obj != null) {
			objID = values.getID(obj);
			if (objID == null) {
				return new EmptyCursor<Statement>();
			}
		}

		List<byte[]> contextIDs;
		if (contexts != null && contexts.length == 0) {
			contextIDs = Arrays.asList(new byte[][] { null });
		} else {
			contextIDs = getContextIDs(OpenRDFUtil.notNull(contexts));
		}
		ArrayList<HBaseStatementCursor> perContextIterList = new ArrayList<HBaseStatementCursor>(
				contextIDs.size());

		for (byte[] contextID : contextIDs) {

			if (includeInferred) {
				// Get both explicit and inferred statements
				perContextIterList.add(triples.getTriples(conn, subjID, predID,
						objID, contextID, readTransaction));
			} else {
				// Only get explicit statements
				perContextIterList.add(triples.getTriples(conn, subjID, predID,
						objID, contextID, true, readTransaction));
			}

		}

		if (perContextIterList.size() == 1) {
			return perContextIterList.get(0);
		} else if (perContextIterList.isEmpty()) {
			return EmptyCursor.getInstance();
		} else {
			return new UnionCursor<Statement>(perContextIterList);
		}
	}

	public void setTripleIndexes(String tripleIndexes) {

	}

}
