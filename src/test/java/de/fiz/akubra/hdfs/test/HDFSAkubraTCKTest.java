package de.fiz.akubra.hdfs.test;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.akubraproject.tck.TCKTestSuite;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import de.fiz.akubra.hdfs.HDFSBlobStore;

public class HDFSAkubraTCKTest extends TCKTestSuite {
	private static final Logger log = LoggerFactory.getLogger(HDFSAkubraTCKTest.class);
	private static MiniDFSCluster cluster;

	@BeforeClass
	public static void startCluster() throws Exception {
			File nameDir=new File("target/test-namenode");
			File dataDir=new File("target/test-datanode");
			FileUtils.deleteDirectory(nameDir);
			FileUtils.deleteDirectory(dataDir);
			nameDir.mkdir();
			dataDir.mkdir();	
			Configuration cfg = new Configuration();
			cfg.set("dfs.name.dir", nameDir.getAbsolutePath());
			cfg.set("dfs.data.dir", dataDir.getAbsolutePath());
			cfg.set("fs.default.name","hdfs://localhost:9000/");
			NameNode.format(cfg);
			cluster = new MiniDFSCluster(9000,cfg,1,false,false,StartupOption.FORMAT,null);
			cluster.waitActive();
		
	}

	@AfterClass
	public static void stopCluster() throws Exception {
		cluster.shutdownDataNodes();
		cluster.shutdownNameNode();
		cluster.shutdown();
	}

	public HDFSAkubraTCKTest() {
		super(new HDFSBlobStore(URI.create("hdfs://localhost:9000/")), URI.create("hdfs://localhost:9000/"), false, true, true, true, true, true, false);
	}

	@Override
	protected URI[] getAliases(URI arg0) {
		return new URI[] { arg0 };
	}

	@Override
	protected URI getInvalidId() {
		return URI.create("urifoo:zt");
	}

	@Override
	protected URI createId(String name) {
		return URI.create(store.getId() + name);
	}

	@Override
	protected String getPrefixFor(String name) {
		return name;
	}
}
