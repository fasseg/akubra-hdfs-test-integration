package de.fiz.akubra.hdfs.test;

import java.net.URI;

import org.akubraproject.Blob;
import org.akubraproject.BlobStore;
import org.akubraproject.BlobStoreConnection;
import org.akubraproject.tck.TCKTestSuite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
		super(store, storeId, false, true);
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
		return URI.create("file:" + name);
	}

	@Override
	protected String getPrefixFor(String name) {
		return name;
	}

	@Override
	public void testClosedConnection() throws Exception {
		// dont run this test since the connection semantices are different
		// super.testClosedConnection();
	}

	@Override
	@Test(expectedExceptions = { UnsupportedOperationException.class })
	public void testSync() throws Exception {
		super.testSync();
	}

	@Test(groups = { "connection", "manipulatesBlobs" }, dependsOnGroups = { "init" })
	public void testListBlobs() throws Exception {
		// check if list-ids is supported
		if (!isListIdsSupp) {
			shouldFail(new ConAction() {
				public void run(BlobStoreConnection con) throws Exception {
					con.listBlobIds(null);
				}
			}, UnsupportedOperationException.class, null);

			return;
		}

		// run the tests
		final URI id1 = createId("blobBasicList1");
		final URI id2 = createId("blobBasicList2");

		listBlobs(getPrefixFor("blobBasicList"), new URI[] {});
		listBlobs(getPrefixFor("blobBasicLisT"), new URI[] {});
		listBlobs(getPrefixFor("blobBasicList2"), new URI[] {});

		createBlob(id1, "hello", true);
		listBlobs(getPrefixFor("blobBasicList"), new URI[] { id1 });
		listBlobs(getPrefixFor("blobBasicLisT"), new URI[] {});
		listBlobs(getPrefixFor("blobBasicList2"), new URI[] {});

		createBlob(id2, "bye", true);
		listBlobs(getPrefixFor("blobBasicList"), new URI[] { id1, id2 });
		listBlobs(getPrefixFor("blobBasicLisT"), new URI[] {});
		listBlobs(getPrefixFor("blobBasicList2"), new URI[] { id2 });

		deleteBlob(id1, "hello", true);
		listBlobs(getPrefixFor("blobBasicList"), new URI[] { id2 });
		listBlobs(getPrefixFor("blobBasicLisT"), new URI[] {});
		listBlobs(getPrefixFor("blobBasicList2"), new URI[] { id2 });

		deleteBlob(id2, "bye", true);
		listBlobs(getPrefixFor("blobBasicList"), new URI[] {});
		listBlobs(getPrefixFor("blobBasicLisT"), new URI[] {});
		listBlobs(getPrefixFor("blobBasicList2"), new URI[] {});

		// test that blobs created/deleted as part of the current txn are
		// properly shown
		runTests(new ConAction() {
			public void run(BlobStoreConnection con) throws Exception {
				Blob b1 = getBlob(con, id1, null);
				Blob b2 = getBlob(con, id2, null);

				listBlobs(con, getPrefixFor("blobBasicList"), new URI[] {});
				listBlobs(con, getPrefixFor("blobBasicLisT"), new URI[] {});
				listBlobs(con, getPrefixFor("blobBasicList2"), new URI[] {});

				createBlob(con, b1, "quibledyqwak");
				listBlobs(con, getPrefixFor("blobBasicList"), new URI[] { id1 });
				listBlobs(con, getPrefixFor("blobBasicLisT"), new URI[] {});
				listBlobs(con, getPrefixFor("blobBasicList1"), new URI[] { id1 });

				createBlob(con, b2, "waflebleeblegorm");
				listBlobs(con, getPrefixFor("blobBasicList"), new URI[] { id1, id2 });
				listBlobs(con, getPrefixFor("blobBasicLisT"), new URI[] {});
				listBlobs(con, getPrefixFor("blobBasicList2"), new URI[] { id2 });

				deleteBlob(con, b1);
				listBlobs(con, getPrefixFor("blobBasicList"), new URI[] { id2 });
				listBlobs(con, getPrefixFor("blobBasicLisT"), new URI[] {});
				listBlobs(con, getPrefixFor("blobBasicList2"), new URI[] { id2 });

				deleteBlob(con, b2);
				listBlobs(con, getPrefixFor("blobBasicList"), new URI[] {});
				listBlobs(con, getPrefixFor("blobBasicLisT"), new URI[] {});
				listBlobs(con, getPrefixFor("blobBasicList1"), new URI[] {});
			}
		});
	}

}
