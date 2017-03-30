package org.openrdf.sail.hbase.data;

import java.io.IOException;
import java.util.Arrays;
import java.util.zip.CRC32;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.openrdf.sail.hbase.HBaseStore;
import org.openrdf.sail.hbase.ValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ValueTable {
	private static final Logger logger = LoggerFactory.getLogger(ValueTable.class);
	private final CRC32 crc32 = new CRC32();

	private static final byte VALUE_TYPE_MASK = 0x03; // 0011
	public static final byte URI_VALUE = 0x00; // 0000
	public static final byte LITERAL_VALUE = 0x01; // 0010
	public static final byte BNODE_VALUE = 0x02; // 0001
	public static final byte LONG_LITERAL_VALUE = 0x03; // 0011
	public static final int LONG_VALUE_LENGTH = 256;
	
	public static final byte NUM_VALUE_BYTES = Bytes.SIZEOF_INT;
	public static final byte[] NULL_VALUE = new byte[NUM_VALUE_BYTES];
	public static final byte[] NULL_CONTEXT = new byte[] { 0, 0, 0, 1 };
	public static final byte[] FIRST_VALUE = new byte[] { 0, 0, 0, 2 };
	public static final byte[] MAX_VALUE = GetMaxValue();
	
	private static byte[] GetMaxValue()
	{
		byte[] maxValue = new byte[NUM_VALUE_BYTES];
		Arrays.fill(maxValue, (byte)0xff);
		return maxValue;
	}
	
	private final HBaseStore store;
	private final String tableName;
	
	
	public ValueTable(HBaseStore store)
	{
		this.store = store;
		this.tableName = HBaseTableFactory.getValueTableName(store.getHBaseStoreConfig().getCatalogName());
	}
	
	public String getTableName()
	{
		return this.tableName;
	}
	
	public byte[] getID(byte[] data) throws IOException {
		return this.getID(data, (byte)-1, false);
	}

	public byte[] getID(byte[] data, byte type, boolean create)
			throws IOException {

		int hash = getDataHash(data);
		byte[] id = Bytes.toBytes(hash);

		// zero out the least significant 4 bits so we can assign sequentially
		// from here
		id[id.length - 1] &= 0xF0;
		
		byte[][] families = ValueTable.getFamilies(id, type);
		
		HTable table = this.store.getHTable(this.tableName);
		Result r = HBaseTable.get(table, id, families);
		if (r.raw().length > 0) {
			for (KeyValue keyValue : r.raw()) {
				if (Bytes.equals(keyValue.getValue(), data)) {
					id[id.length - 1] |= keyValue.getBuffer()[keyValue.getQualifierOffset()];
					return id;
				}
			}
		}

		if (create) {
			if (r.raw().length < 0x0F) {
				// we don't want to use 0 or 1 as a valid id
				byte nextSequentialId = hash == 0 ? (byte)(r.raw().length + 2) : (byte)r.raw().length;

				HBaseTable.put(table, id, families[0], new byte[] { nextSequentialId }, data);
				this.store.putHTable(table);
				
				id[id.length - 1] |= nextSequentialId;
				return id;
			} else {
				logger.error("All value IDs beginning:"
							+ (hash & 0xF0)
							+ " are in use. This wasn't supposed to be possible...");
			}
		}
		
		this.store.putHTable(table);

		return null;
	}
	
	private int getDataHash(byte[] data) {
		synchronized (crc32) {
			crc32.update(data);
			int crc = (int) crc32.getValue();
			crc32.reset();
			return crc;
		}
	}
	
	private static byte[][] getFamilies(byte[] id, byte type)
	{
		if (type != -1) {
			byte family = (byte)
				(type == ValueTable.URI_VALUE ? id[id.length - 2] & 0x0F :
					type == ValueTable.LITERAL_VALUE ? (HBaseTableFactory.NUM_URI_LITERAL_FAMILIES >> 1) + (id[id.length - 2] & 0x0F) :
						type == ValueTable.BNODE_VALUE ? HBaseTableFactory.NUM_URI_LITERAL_FAMILIES :
							HBaseTableFactory.NUM_URI_LITERAL_FAMILIES + 1);
			
			return new byte[][] { HBaseTableFactory.FAMILY_NAMES[family] };
		}
		else {
			return new byte[][] { 		
				HBaseTableFactory.FAMILY_NAMES[(byte)(id[id.length - 2] & 0x0F)],
				HBaseTableFactory.FAMILY_NAMES[(byte)((HBaseTableFactory.NUM_URI_LITERAL_FAMILIES >> 1) + (id[id.length - 2] & 0x0F))],
				HBaseTableFactory.FAMILY_NAMES[HBaseTableFactory.NUM_URI_LITERAL_FAMILIES],
				HBaseTableFactory.FAMILY_NAMES[HBaseTableFactory.NUM_URI_LITERAL_FAMILIES + 1]
			};
		}
			
	}
	
	public static byte getType(KeyValue kv)
	{
		if (Bytes.compareTo(kv.getBuffer(), kv.getFamilyOffset(), kv.getFamilyLength(), HBaseTableFactory.FAMILY_NAMES[HBaseTableFactory.NUM_URI_LITERAL_FAMILIES], 0, 2) == 0) {
			return ValueTable.BNODE_VALUE;
		}
		else if (Bytes.compareTo(kv.getBuffer(), kv.getFamilyOffset(), kv.getFamilyLength(), HBaseTableFactory.FAMILY_NAMES[HBaseTableFactory.NUM_URI_LITERAL_FAMILIES + 1], 0, 2) == 0) {
			return ValueTable.LONG_LITERAL_VALUE;
		}
		else if (Bytes.compareTo(kv.getBuffer(), kv.getFamilyOffset(), 1, HBaseTableFactory.FAMILY_NAMES[0], 0, 1) == 0) {
			return ValueTable.URI_VALUE;
		}
		else {
			return ValueTable.LITERAL_VALUE;
		}
	}

	public byte[] putData(byte[] data, byte type) throws IOException {

		return getID(data, type, true);
	}

	public KeyValue getData(byte[] internalID, byte type)
			throws IOException {

		byte[] key = new byte[internalID.length];
		Bytes.putBytes(key, 0, internalID, 0, key.length);
		key[key.length - 1] &= 0xF0;
		
		byte[][] families = ValueTable.getFamilies(internalID, type);
		
		HTable table = this.store.getHTable(this.tableName);
		Result result = HBaseTable.get(table, key, families);
		this.store.putHTable(table);

		byte[] qualifier = new byte[] { (byte)(internalID[internalID.length - 1] & 0x0F) };
		for (KeyValue kv : result.raw())
			if (kv.getBuffer()[kv.getQualifierOffset()] == qualifier[0])
				return kv;

		return null;
	}
	
	public void clear() throws IOException
	{
		HTable table = this.store.getHTable(this.tableName);
		HBaseTable.clear(table, store.getHBaseConfiguration());
		this.store.putHTable(table);
	}
}
