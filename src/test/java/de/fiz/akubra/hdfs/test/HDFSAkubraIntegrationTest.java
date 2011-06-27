package de.fiz.akubra.hdfs.test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Random;
import java.util.UUID;

import org.akubraproject.Blob;
import org.akubraproject.BlobStore;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import de.fiz.akubra.hdfs.HDFSBlobStore;

public class HDFSAkubraIntegrationTest {
    private static BlobStore store;
    private static URI storeId;
    private static final Logger log = LoggerFactory.getLogger(HDFSAkubraTCKTest.class);

    public HDFSAkubraIntegrationTest() {
        storeId = URI.create("hdfs://localhost:9000/hdfs-test-integration/");
        store = new HDFSBlobStore(storeId);
    }

    @Test
    public void testCreateBlob() throws Exception {
        Blob b = createRandomBlob(store, 4096);
        assertTrue(b.exists());
    }

    @Test
    public void testMoveBlob() throws Exception {
        Blob b = createRandomBlob(store, 4096);
        URI toUri = URI.create(storeId.toASCIIString() + "testMove");
        assertTrue(b.exists());
        b = b.moveTo(toUri, null);
        assertEquals(b.getId(), toUri);
        assertTrue(b.exists());
        b.delete();
        assertFalse(b.exists());
    }

    private Blob createRandomBlob(BlobStore store, int size) throws IOException {
        Blob b = store.openConnection(null, null).getBlob(URI.create(store.getId().toString() + UUID.randomUUID().toString()), null);
        byte[] data = new byte[4096];
        new Random().nextBytes(data);
        IOUtils.copy(new ByteArrayInputStream(data), b.openOutputStream(size, false));
        return b;
    }

    @AfterClass
    public static void cleanup() throws Exception {
        FileSystem hdfs = FileSystem.get(storeId, new Configuration());
        hdfs.delete(new Path("hdfs://localhost:9000/hdfs-test-integration/"), true);
    }
}
