package fr.inria.atlanmod.atl_mr.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.eclipse.emf.common.util.URI;

import fr.inria.atlanmod.neoemf.util.NeoEMFUtil;


public class HbaseTracer implements Tracer{

	protected static final byte[] EINVERSE_FAMILY = Bytes.toBytes("inv");
	protected static final byte[] TARGET_COLUMN = Bytes.toBytes("trg");

	protected HConnection traceConnection;
	protected HTable table;
	protected URI traceURI;

	public HbaseTracer (URI traceURI) {

		this.traceURI = traceURI;

		Configuration conf = HBaseConfiguration.create();
		//TODO: add a configuration parser to the handle user specific conf
		//TODO: hbase.zookeeper.quorum takes a list of
		conf.set("hbase.zookeeper.quorum", traceURI.host());
		conf.set("hbase.zookeeper.property.clientPort", traceURI.port() != null ? traceURI.port() : "2181");

		// setting up tehe table name
		byte[] tableName = Bytes.toBytes(NeoEMFUtil.formatURI(traceURI.appendSegment("map")));
		HBaseAdmin admin;
		try {
			admin = new HBaseAdmin(conf);
			traceConnection =  HConnectionManager.createConnection(conf);

			if (!admin.tableExists(tableName)) {
				@SuppressWarnings("deprecation")
				HTableDescriptor desc = new HTableDescriptor(tableName);
				HColumnDescriptor inverseFamily = new HColumnDescriptor(EINVERSE_FAMILY);
				desc.addFamily(inverseFamily);
				admin.createTable(desc);
			}
			table = new HTable(conf, tableName);
			admin.close();
		} catch (IOException e) {
			e.printStackTrace();

		}
	}

	protected byte[] toPrettyBytes( String [] strings) {
		try {
			ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
			ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
			objectOutputStream.writeObject(strings);
			objectOutputStream.flush();
			objectOutputStream.close();
			return byteArrayOutputStream.toByteArray();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}


	protected static String[] toPrettyStrings(byte[] bytes) {
		if (bytes == null) {
			return null;
		}
		String[] result = null;
		ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
		ObjectInputStream objectInputStream = null;
		try {
			objectInputStream = new ObjectInputStream(byteArrayInputStream);
			result = (String[]) objectInputStream.readObject();

		} catch (IOException e) {

		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeQuietly(objectInputStream);
		}
		return result;

	}
}