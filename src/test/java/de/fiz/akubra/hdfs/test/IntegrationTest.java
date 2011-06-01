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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Iterator;
import java.util.Random;

import org.akubraproject.DuplicateBlobException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.junit.Before;
import org.junit.Test;

import de.fiz.akubra.hdfs.HDFSBlob;
import de.fiz.akubra.hdfs.HDFSBlobStore;
import de.fiz.akubra.hdfs.HDFSBlobStoreConnection;

public class IntegrationTest {
	HDFSBlobStore store;
	
	@Before
	public void init() throws Exception {
		store = new HDFSBlobStore("hdfs://localhost:9000/akubra-hdfs-integration-test/");
	}

	@Test
	public void testHDFSBlobStore1() throws Exception {
		HDFSBlob blob = createRandomBlob(4096, store);
		assertNotNull(blob);
		assertTrue(blob.getSize() == 4096);
		System.out.println("created " + blob.getId());
		HDFSBlob newBlob = (HDFSBlob) blob.moveTo(new URI("hdfs://localhost:9000/akubra-hdfs-integration-test/int_test.example"), null);
		assertTrue(newBlob.exists());
		assertTrue(newBlob.getSize() == 4096);
		assertFalse(blob.exists());
		newBlob.delete();
		assertFalse(newBlob.exists());
	}
	@Test
	public void testMoveBlob() throws Exception {
		byte[] orig=createRandomData(8192);
		HDFSBlob blob = (HDFSBlob) store.openConnection(null, null).getBlob(new ByteArrayInputStream(orig),8192,null);
		ByteArrayOutputStream out=new ByteArrayOutputStream();
		IOUtils.copy(blob.openInputStream(), out);
		byte[] target=out.toByteArray();
		assertArrayEquals(orig,target);
		blob.delete();
	}

	@Test
	public void testOverwriteBlob() throws Exception {
		byte[] orig=createRandomData(8192);
		HDFSBlob blob = (HDFSBlob) store.openConnection(null, null).getBlob(new ByteArrayInputStream(orig),8192,null);
		orig=createRandomData(5687);
		OutputStream blobOut=blob.openOutputStream(0, true);
		IOUtils.copy(new ByteArrayInputStream(orig),blobOut);
		blobOut.close();
		ByteArrayOutputStream out=new ByteArrayOutputStream();
		IOUtils.copy(blob.openInputStream(), out);
		byte[] target=out.toByteArray();
		assertArrayEquals(orig,target);
	}

	@Test(expected=DuplicateBlobException.class)
	public void testDontOverwriteBlob() throws Exception {
		byte[] orig=createRandomData(8192);
		HDFSBlob blob = (HDFSBlob) store.openConnection(null, null).getBlob(new ByteArrayInputStream(orig),8192,null);
		orig=createRandomData(5687);
		blob.openOutputStream(0, false);
	}

	@Test
	public void testBlobIterator() throws Exception {
		HDFSBlobStoreConnection conn=(HDFSBlobStoreConnection) store.openConnection(null, null);
		Iterator<URI> it=conn.listBlobIds("/akubra-hdfs-integration-test");
		while(it.hasNext()){
			HDFSBlob blob=(HDFSBlob) conn.getBlob(it.next(), null);
			assertTrue(blob.exists());
			blob.delete();
			assertFalse(blob.exists());
		}
	}

	private HDFSBlob createRandomBlob(int size, HDFSBlobStore store) throws IOException {
		ByteArrayInputStream in = new ByteArrayInputStream(createRandomData(size));
		return (HDFSBlob) store.openConnection(null, null).getBlob(in, size, null);
	}
	private byte[] createRandomData(int size){
		byte[] buf = new byte[size];
		new Random().nextBytes(buf);
		return buf;
	}

}
