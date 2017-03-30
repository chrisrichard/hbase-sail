package org.openrdf.sail.hbase.data;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;
import org.openrdf.sail.hbase.HBaseStore;
import org.openrdf.sail.hbase.config.HBaseStoreConfig;
import org.openrdf.sail.hbase.config.IndexSpec;
import org.openrdf.store.StoreException;

public class HBaseTableFactory {
	
	private static final String NAMESPACES = "namespaces";
	public static final byte NUM_NAMESPACE_FAMILIES = 1;

	private static final String VALUES = "values";
	public static final byte NUM_URI_LITERAL_FAMILIES = 32;
	public static final byte NUM_VALUE_FAMILIES = NUM_URI_LITERAL_FAMILIES + 2;
	
	private static final String TRIPLES = "triples";
	
	public static final int MAX_FAMILIES = 128;
	
	public static final byte[][] FAMILY_NAMES = getFamilyNames();
	private static byte[][] getFamilyNames() {
		
		byte[][] familyNames = new byte[MAX_FAMILIES][];
		for (int i = 0; i < familyNames.length; ++i)
			familyNames[i] = Bytes.toBytes(StringUtils.byteToHexString(new byte[] { (byte)i }));
		
		return familyNames;
	}
	
	public static byte getFamilyByte(byte[] familyName)
	{
		byte familyByte = 0;
		for (int i = 0; i < 2; ++i)
			if (familyName[i] >= 0x30 && familyName[i] <= 0x39)
				familyByte |= (familyName[i] - 0x30) << ((1 - i) * 4);
			else if (familyName[i] >= 0x61 && familyName[i] <= 0x66)
				familyByte |= (familyName[i] - 0x51) << ((1 - i) * 4);
			else
				throw new IllegalArgumentException("Argument 'familyName' contains an invalid value: " + familyName[i]);
		
		return familyByte;				
	}
	
	public static final byte[][][][] FAMILIES = getFamilies();
	private static byte[][][][] getFamilies() {
		
		byte[][][][] families = new byte[MAX_FAMILIES][MAX_FAMILIES][][];
		for (int i = 0; i < families.length; ++i) {
			
			for (int j = 0; j < MAX_FAMILIES; ++j) {
				if ((i & j) == j) {
					int numFamilies = 1 << (Integer.bitCount(~i & 0xFF) - (Byte.SIZE - Integer.bitCount(MAX_FAMILIES - 1)));
					families[i][j] = new byte[numFamilies][];
					
					int lastFamily = 0;
					for (int k = 0; k < MAX_FAMILIES; ++k) {
						if ((k & i) == j) {
							families[i][j][lastFamily] = FAMILY_NAMES[k];
							++lastFamily;
						}
					}
				}
			}
		}		
		
		return families;
	}
	
	private HBaseStore store;
	private HBaseStoreConfig storeConf;
	
	private HTablePool tablePool;
	private HBaseAdmin admin;
	
	public HBaseTableFactory(HBaseStore store, HBaseStoreConfig conf, HBaseConfiguration hbaseConf) throws IOException
	{
		this.store = store;
		this.storeConf = conf;
		
		this.tablePool = new HTablePool();
		this.admin = new HBaseAdmin(hbaseConf);		
	}
	
	public TripleTable getTripleTable()
			throws IOException, StoreException {
		
		for (IndexSpec indexSpec : storeConf.getTripleIndexes())
		{	
			byte familyBits = 0;
			for (byte numBits : indexSpec.getFamilyFieldBits())
				familyBits += numBits;
			String tableName = HBaseTableFactory.getTripleTableName(storeConf.getCatalogName(), new String(indexSpec.getFieldSeq()));
			if (!admin.tableExists(tableName)) {
				createHTable(tableName, (byte)(1 << familyBits));
			} else if (!admin.isTableEnabled(tableName)) {
				admin.enableTable(tableName);
			}
		}

		return new TripleTable(store);
	}
	
	public static String getTripleTableName(String catalogName, String fieldSeq)
	{
		return (catalogName != null ? (catalogName + "-") : "") + TRIPLES + "-" + fieldSeq;
	}
	
	public ValueTable getValueTable()
			throws IOException {
		
		String tableName = HBaseTableFactory.getValueTableName(storeConf.getCatalogName());
		if (!admin.tableExists(tableName)) {
			createHTable(tableName, NUM_VALUE_FAMILIES);
		} else if (!admin.isTableEnabled(tableName)) {
			admin.enableTable(tableName);
		}

		return new ValueTable(store);		
	}
	
	public static String getValueTableName(String catalogName)
	{
		return (catalogName != null ? (catalogName + "-") : "") + VALUES;
	}
		
	public NamespaceTable getNamespaceTable()
			throws IOException {
		
		String tableName = HBaseTableFactory.getNamespaceTableName(storeConf.getCatalogName());
		if (!admin.tableExists(tableName)) {
			createHTable(tableName, NUM_NAMESPACE_FAMILIES);
		} else if (!admin.isTableEnabled(tableName)) {
			admin.enableTable(tableName);
		}

		return new NamespaceTable(store);
	}
	
	public static String getNamespaceTableName(String catalogName)
	{
		return (catalogName != null ? (catalogName + "-") : "") + NAMESPACES;
	}
		
	private void createHTable(String tableName, byte numFamilies) 
			throws IOException {
		
		HTableDescriptor desc = new HTableDescriptor(tableName);
		
		for (byte i = 0; i < numFamilies; ++i) {

			desc.addFamily(
				new HColumnDescriptor(
					HBaseTableFactory.FAMILY_NAMES[i],
					HColumnDescriptor.DEFAULT_VERSIONS,
					HColumnDescriptor.DEFAULT_COMPRESSION, 
					HColumnDescriptor.DEFAULT_IN_MEMORY,
					HColumnDescriptor.DEFAULT_BLOCKCACHE,
					HColumnDescriptor.DEFAULT_TTL, 
					false)); 		
			};
		
		admin.createTable(desc);
	}

	public HTablePool getTablePool() {
		return this.tablePool;
	}	
}
