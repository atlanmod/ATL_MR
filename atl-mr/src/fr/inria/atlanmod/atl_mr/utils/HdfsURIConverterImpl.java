package fr.inria.atlanmod.atl_mr.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.resource.impl.ExtensibleURIConverterImpl;

public class HdfsURIConverterImpl extends ExtensibleURIConverterImpl {
	@Override
	public InputStream createInputStream(URI uri, java.util.Map<?, ?> options) throws IOException {
		Path path = new Path(uri.toString());
		FileSystem fileSystem = FileSystem.get(path.toUri(), new Configuration());
		return fileSystem.open(path);
	}

	@Override
	public OutputStream createOutputStream(URI uri, java.util.Map<?, ?> options) throws IOException {
		Path path = new Path(uri.toString());
		FileSystem fileSystem = FileSystem.get(path.toUri(), new Configuration());
		return fileSystem.create(path);
	}
}