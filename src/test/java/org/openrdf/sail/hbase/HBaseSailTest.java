package org.openrdf.sail.hbase;

import org.openrdf.sail.RDFStoreTest;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.hbase.config.HBaseStoreConfig;
import org.openrdf.store.StoreException;


public class HBaseSailTest extends RDFStoreTest {

	public HBaseSailTest(String name) {
		super(name);
	}

	@Override
	protected Sail createSail() throws StoreException
	{
		Sail sail = new HBaseStore(new HBaseStoreConfig("test"));
		sail.initialize();
		
		SailConnection conn = sail.getConnection();
		try {
			conn.removeStatements(null, null, null);
			conn.clearNamespaces();
		}
		finally {
			conn.close();
		}
		
		return sail;
	}
}
