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

import java.net.URI;
import java.net.URISyntaxException;

import javax.transaction.Transaction;

import org.akubraproject.tck.TCKTestSuite;

import de.fiz.akubra.hdfs.HDFSBlobStore;
import de.fiz.akubra.hdfs.HDFSBlobStoreConnection;

public class AkubraTCKSuite extends TCKTestSuite {

	public AkubraTCKSuite(HDFSBlobStore store) throws Exception {
		super(store,store.getId(), false, true, true, true, true, true, false);
	}

	@Override
	protected String getPrefixFor(String name) {
		return "hdfs://localhost:9000/";
	}

	@Override
	protected URI[] getAliases(URI uri) {
		return null;
	}

	@Override
	protected URI getInvalidId() {
		try {
			return new URI("file://mine/");
		} catch (URISyntaxException e) {
			e.printStackTrace();
			return null;
		}
	}

	public static void main(String[] args) {
		try {
			final HDFSBlobStore store=new HDFSBlobStore("hdfs://localhost:9000");
			AkubraTCKSuite suite = new AkubraTCKSuite(store);
			suite.runTests(new Action() {
				public void run(Transaction txn) throws Exception {
					HDFSBlobStoreConnection conn=(HDFSBlobStoreConnection) store.openConnection(null, null);
					conn.close();
				}
			},true);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
