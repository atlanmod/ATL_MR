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
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.eclipse.emf.common.util.URI;


public class HbaseTracer implements Tracer{

	protected static final byte[] EINVERSE_FAMILY = Bytes.toBytes("inv");
	protected static final byte[] TARGET_COLUMN = Bytes.toBytes("trg");

	protected Connection traceConnection;
	protected Table table;
	protected URI traceURI;

	public HbaseTracer (URI traceURI) {

		this.traceURI = traceURI;

		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", traceURI.host());
		conf.set("hbase.zookeeper.property.clientPort", traceURI.port() != null ? traceURI.port() : "2181");
		TableName tableName = TableName.valueOf(traceURI.devicePath().replaceAll("/", "_").concat("_map"));

		try {

			traceConnection =  ConnectionFactory.createConnection(conf);
			if (!traceConnection.getAdmin().tableExists(tableName)) {
				HTableDescriptor desc = new HTableDescriptor(tableName);
				HColumnDescriptor inverseFamily = new HColumnDescriptor(EINVERSE_FAMILY);
				desc.addFamily(inverseFamily);
				traceConnection.getAdmin().createTable(desc);
			}
			table = traceConnection.getTable(tableName);

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