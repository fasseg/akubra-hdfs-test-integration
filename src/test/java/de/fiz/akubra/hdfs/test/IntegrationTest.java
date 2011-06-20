/*
   Copyright 2011 FIZ Karlsruhe 

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */
package de.fiz.akubra.hdfs.test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Iterator;
import java.util.Random;

import org.akubraproject.Blob;
import org.akubraproject.BlobStoreConnection;
import org.akubraproject.DuplicateBlobException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import de.fiz.akubra.hdfs.HDFSBlobStore;

public class IntegrationTest {
	HDFSBlobStore store;
	private static final URI STORE_URI = URI
			.create("hdfs://localhost:9000/akubra-hdfs-integration-test/");
	private static final Logger log = LoggerFactory
			.getLogger(IntegrationTest.class);

	@BeforeTest
	public void init() throws Exception {
		store = new HDFSBlobStore(STORE_URI);
	}

	@Test
	public void testHDFSBlobStore1() throws Exception {
		Blob blob = createRandomBlob(4096, store);
		assertNotNull(blob);
		assertTrue(blob.getSize() == 4096);
		Blob newBlob = blob.moveTo(URI.create(store.getId() + "int_test.example"), null);
		assertTrue(newBlob.exists());
		assertTrue(newBlob.getSize() == 4096);
		newBlob.delete();
		assertFalse(newBlob.exists());
	}

	@Test
	public void testMoveBlob1() throws Exception {
		Blob blob=createRandomBlob(2048, store);
		assertNotNull(blob);
		URI newId=URI.create(store.getId().toASCIIString() + "moved/testMove");
		blob.moveTo(newId, null);
	}

	@Test
	public void testMoveBlob2() throws Exception {
		Blob blob=createRandomBlob(1024*128, store);
		assertNotNull(blob);
		URI newId=URI.create(store.getId().toASCIIString() + "moved/in/new/deep/directory/only/names/though/testMove");
		blob.moveTo(newId, null);
	}

	@Test
	public void testOverwriteBlob() throws Exception {
		byte[] orig = createRandomData(8192);
		Blob blob = store.openConnection(null, null).getBlob(
				new ByteArrayInputStream(orig), 8192, null);
		orig = createRandomData(5687);
		OutputStream blobOut = blob.openOutputStream(0, true);
		IOUtils.copy(new ByteArrayInputStream(orig), blobOut);
		blobOut.close();
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		IOUtils.copy(blob.openInputStream(), out);
		byte[] target = out.toByteArray();
		assertEquals(orig, target);
	}

	@Test(expectedExceptions = { DuplicateBlobException.class })
	public void testDontOverwriteBlob() throws Exception {
		byte[] orig = createRandomData(8192);
		Blob blob = store.openConnection(null, null).getBlob(
				new ByteArrayInputStream(orig), 8192, null);
		orig = createRandomData(5687);
		blob.openOutputStream(0, false);
	}

	@Test
	public void testBlobIterator() throws Exception {
		BlobStoreConnection conn = store.openConnection(null, null);
		Iterator<URI> it = conn.listBlobIds("/akubra-hdfs-integration-test");
		while (it.hasNext()) {
			Blob blob = conn.getBlob(it.next(), null);
			assertTrue(blob.exists());
			blob.delete();
			assertFalse(blob.exists());
		}
	}

	@AfterClass
	public void cleanUp() throws Exception {
		FileSystem fs = FileSystem.get(STORE_URI, new Configuration());
		fs.delete(new Path(STORE_URI.toASCIIString()), true);
	}

	private Blob createRandomBlob(int size, HDFSBlobStore store)
			throws IOException {
		ByteArrayInputStream in = new ByteArrayInputStream(
				createRandomData(size));
		return store.openConnection(null, null).getBlob(in, size, null);
	}

	private byte[] createRandomData(int size) {
		byte[] buf = new byte[size];
		new Random().nextBytes(buf);
		return buf;
	}

}
