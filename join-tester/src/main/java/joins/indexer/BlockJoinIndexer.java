package joins.indexer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;

public class BlockJoinIndexer extends JoinIndexer {

	public BlockJoinIndexer() throws Exception {
		super();
	}

	@Override
	protected void addChild(SolrInputDocument doc, SolrInputDocument child) {
		SolrInputField field = doc.getField("children");
		if(field==null){
			doc.addField("children", new ArrayList<SolrInputDocument>());
			field = doc.getField("children");
		}
		field.getValues().add(child);
	}
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		main(new BlockJoinIndexer());  
	}

	@Override
	protected void addDocs(List<SolrInputDocument> list) throws SolrServerException,
			IOException {
		UpdateRequest req = new UpdateRequest();
				req.add(list);
				req.setParam("update.chain","flatten-chain");
				req.setCommitWithin(-1);
		req.process(_server);
	}
}
