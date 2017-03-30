package org.openrdf.sail.hbase.data;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseTable {
	private static final Logger logger = LoggerFactory.getLogger(HBaseTable.class);
	
    public static void put(HTable table, byte[] key, byte[] family, byte[] qualifier, byte[] value) throws IOException {
    	
    	logger.info("Putting to table: " + Bytes.toString(table.getTableName()) + ", autoFlush: " + table.isAutoFlush() + ", key: " + StringUtils.byteToHexString(key) + ", family: " + StringUtils.byteToHexString(family) + ", qualifier: " + (qualifier != null ? StringUtils.byteToHexString(qualifier) : "null") + ", value: " + (value != null ? StringUtils.byteToHexString(value) : "null") + ".");
		table.put(HBaseTable.getPut(key, family, qualifier, value));
	}
    
    public static Put getPut(byte[] key, byte[] family, byte[] qualifier, byte[] value)
    {
    	Put p = new Put(key);
		p.add(family, qualifier, value);
		return p;	    	
    }
        
    public static void delete(HTable table, byte[] key, byte[] family, byte[] qualifier) throws IOException {

    	logger.info("Deleting from table: " + Bytes.toString(table.getTableName()) + ", autoFlush: " + table.isAutoFlush() + ", key: " + StringUtils.byteToHexString(key) + ", family: " + StringUtils.byteToHexString(family) + ", qualifier: " + (qualifier != null ? StringUtils.byteToHexString(qualifier) : "null") + ".");
    	table.delete(HBaseTable.getDelete(key, family, qualifier));
    }
    
    public static Delete getDelete(byte[] key, byte[] family, byte[] qualifier)
    {
    	Delete d = new Delete(key);
    	d.deleteColumns(family, qualifier);
    	
    	return d;
    }
        
    public static Result get(HTable table, byte[] key, byte[] family) throws IOException
    {
    	return HBaseTable.get(table, key, family, null);
    }
    
    public static Result get(HTable table, byte[] key, byte[] family, byte[] qualifier) throws IOException
    {
    	Get g = new Get(key);
    	
    	if (family != null && qualifier != null)
    		g.addColumn(family, qualifier);
    	else if (family != null) 
    		g.addFamily(family);
    	
    	return table.get(g);    	
    }
    
    public static Result get(HTable table, byte[] key, byte[][] families) throws IOException
    {
    	Get g = new Get(key);
    	
    	if (families != null)
    		for (byte[] family : families)
    			if (family != null)
    				g.addFamily(family);
    	
    	return table.get(g);    	    
    }
    
    public static Result get(HTable table, byte[] key, byte[][] families, byte[] qualifier) throws IOException
    {
    	Get g = new Get(key);

    	if (families != null)
    		for (byte[] family : families)
		    	if (qualifier != null)
		    		g.addColumn(family, qualifier);
		    	else 
		    		g.addFamily(family);    			
    	
    	return table.get(g);    	    
    }
    
    public static ResultScanner scan(HTable table, byte[] start, byte[] stop, byte[][] families) throws IOException {
    	return HBaseTable.scan(table, start, stop, families, null, null);
    }
    
	public static ResultScanner scan(HTable table, byte[] start, byte[] stop, byte[][] families, byte[] qualifier, Filter filter) throws IOException {
		
		Scan s = new Scan();
		s.setCaching(32);
		
		if (start != null)
			s.setStartRow(start);
		if (stop != null)
			s.setStopRow(stop);
		
		if (families != null)
			for (byte[] family : families)
				if (qualifier == null)
					s.addFamily(family);
				else
					s.addColumn(family, qualifier);
		
		if (filter != null)
			s.setFilter(filter);
		
		logger.info("Scanning table: " + Bytes.toString(table.getTableName()) + ", start: " + (start != null ? StringUtils.byteToHexString(start) : "null") + ", stop: " + (stop != null ? StringUtils.byteToHexString(stop) : "null") + ".");
		
		return table.getScanner(s);
	}
	
	public static void drop(HTable table, HBaseConfiguration conf) throws IOException {
		
		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.tableExists(table.getTableName())) {
			if (admin.isTableEnabled(table.getTableName()))
				admin.disableTable(table.getTableName());
			admin.deleteTable(table.getTableName());
		}
	}

	public static void clear(HTable table, HBaseConfiguration conf) throws IOException {		
		HTableDescriptor desc = table.getTableDescriptor();
		
		HBaseAdmin admin = new HBaseAdmin(conf);
		admin.disableTable(table.getTableName());
		admin.deleteTable(table.getTableName());
		admin.createTable(desc);
	}		
}
