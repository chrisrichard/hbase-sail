package org.openrdf.sail.hbase.data;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.openrdf.model.Namespace;
import org.openrdf.model.impl.NamespaceImpl;
import org.openrdf.sail.hbase.HBaseStore;
import org.openrdf.store.StoreException;

public class NamespaceTable implements Iterable<Namespace> {

	private final HBaseStore store;
	private final String tableName;
	
	// store the namespaces in memory, write-thru
	private final Map<String, Namespace> namespacesMap;

	public NamespaceTable(HBaseStore store)
			throws IOException {

		this.store = store;
		this.tableName = HBaseTableFactory.getNamespaceTableName(store.getHBaseStoreConfig().getCatalogName());
		
		namespacesMap = new LinkedHashMap<String, Namespace>(32);
		//this.readNamespaces();
	}
	
	public String getTableName()
	{
		return this.tableName;
	}

	public String getNamespace(String prefix) {
		String result = null;
		Namespace namespace = namespacesMap.get(prefix);
		if (namespace != null) {
			result = namespace.getName();
		}
		return result;
	}

	public void setNamespace(String prefix, String name) throws IOException {
		Namespace newNamespace = new NamespaceImpl(prefix, name);
		namespacesMap.put(prefix, newNamespace);

		HTable table = this.store.getHTable(this.tableName);
		HBaseTable.put(table, Bytes.toBytes(prefix), HBaseTableFactory.FAMILY_NAMES[0], null, Bytes.toBytes(name));
		this.store.putHTable(table);
	}

	public void removeNamespace(String prefix) throws IOException {
		Namespace ns = namespacesMap.remove(prefix);

		if (ns != null) {

			HTable table = this.store.getHTable(this.tableName);
			HBaseTable.delete(table, Bytes.toBytes(prefix), HBaseTableFactory.FAMILY_NAMES[0], null);
			this.store.putHTable(table);
		}
	}

	protected void readNamespaces() throws IOException {
		HTable table = null;
		HBaseCursor cursor = null;

		try {
			table = this.store.getHTable(this.tableName);
			cursor = new HBaseCursor(HBaseTable.scan(table, null, null, new byte[][] {{ 0 }}));

			while (cursor.hasNext()) {
				KeyValue keyValue = cursor.next();
				String prefix = Bytes.toString(keyValue.getKey());
				namespacesMap.put(prefix, new NamespaceImpl(prefix, Bytes
						.toString(keyValue.getValue())));
			}
		} catch (StoreException se) {
			throw new IOException(se);
		}
		finally
		{
			if (table != null)
				this.store.putHTable(table);
		}
	}

	public Iterator<Namespace> iterator() {
		return namespacesMap.values().iterator();
	}
	
	public void clear() throws IOException
	{
		namespacesMap.clear();		
		
		HTable table = this.store.getHTable(this.tableName);
		HBaseTable.clear(table, store.getHBaseConfiguration());
		this.store.putHTable(table);
	}
}