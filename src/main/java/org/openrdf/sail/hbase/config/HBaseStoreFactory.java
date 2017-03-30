package org.openrdf.sail.hbase.config;

import org.openrdf.sail.Sail;
import org.openrdf.sail.config.SailFactory;
import org.openrdf.sail.config.SailImplConfig;
import org.openrdf.sail.hbase.HBaseStore;
import org.openrdf.store.StoreConfigException;
import org.openrdf.store.StoreException;

public class HBaseStoreFactory implements SailFactory {
	public static final String SAIL_TYPE = "openrdf:HBaseStore";

	/**
	 * Returns the Sail's type: <tt>openrdf:HBaseStore</tt>.
	 */
	public String getSailType() {
		return SAIL_TYPE;
	}

	public SailImplConfig getConfig() {
		return new HBaseStoreConfig();
	}

	public Sail getSail(SailImplConfig config) throws StoreConfigException {
		if (!SAIL_TYPE.equals(config.getType())) {
			throw new StoreConfigException("Invalid Sail type: "
					+ config.getType());
		}

		try {
			if (config instanceof HBaseStoreConfig)
				return new HBaseStore((HBaseStoreConfig) config);
			else
				return new HBaseStore(new HBaseStoreConfig());
		} catch (StoreException se) {
			throw new StoreConfigException(se);
		}
	}
}
