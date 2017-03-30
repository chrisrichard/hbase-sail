package org.openrdf.sail.hbase;

import java.io.IOException;

import org.openrdf.cursor.Cursor;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.query.EvaluationException;
import org.openrdf.query.algebra.evaluation.TripleSource;

public class HBaseTripleSource implements TripleSource {

	/*-----------*
	 * Constants *
	 *-----------*/

	protected final HBaseStore store;
	protected final HBaseConnection conn;

	protected final boolean includeInferred;

	protected final boolean readTransaction;

	/*--------------*
	 * Constructors *
	 *--------------*/

	protected HBaseTripleSource(HBaseStore store, HBaseConnection conn, boolean includeInferred, boolean readTransaction) {
		this.store = store;
		this.conn = conn;
		this.includeInferred = includeInferred;
		this.readTransaction = readTransaction;
	}

	/*---------*
	 * Methods *
	 *---------*/

	public Cursor<? extends Statement> getStatements(Resource subj, URI pred, Value obj, Resource... contexts)
		throws EvaluationException
	{
		try {
			return store.createStatementCursor(conn, subj, pred, obj, includeInferred, readTransaction, contexts);
		}
		catch (IOException e) {
			throw new EvaluationException("Unable to get statements", e);
		}
	}

	public ValueFactory getValueFactory() {
		return store.getValueFactory();
	}
}
