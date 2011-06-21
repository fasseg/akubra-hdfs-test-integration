package de.fiz.akubra.hdfs.test;

import java.net.URI;

import org.akubraproject.BlobStore;
import org.akubraproject.BlobStoreConnection;
import org.akubraproject.tck.TCKTestSuite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import de.fiz.akubra.hdfs.HDFSBlobStore;

@Test
public class HDFSAkubraTCKTest extends TCKTestSuite {
    private static BlobStore store;
    private static URI storeId;
    private static final Logger log = LoggerFactory.getLogger(HDFSAkubraTCKTest.class);

    static {
        storeId = URI.create("hdfs://localhost:9000/");
        store = new HDFSBlobStore(storeId);
    }

    public HDFSAkubraTCKTest() {
        super(store, storeId, false, true, true, true, true, true, false);
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
