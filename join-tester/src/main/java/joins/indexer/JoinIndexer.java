package joins.indexer;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrServer;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;
import java.util.List;

public class JoinIndexer extends ProtoIndexer{
    protected final int COMMIT_WITHIN = 600000;
  private final String SOLR_SERVER_URL = "http://localhost:8983/solr";

  public static void main(String[] args) throws Exception {
	  main(new JoinIndexer());
  }

  public JoinIndexer() throws Exception {

    _server = new ConcurrentUpdateSolrServer(SOLR_SERVER_URL, 4, 2);

    _server.setParser(new XMLResponseParser()); // binary parser is used by default

  }
  
  protected void flushTail() throws SolrServerException, IOException {
     super.flushTail();
      _server.commit();
  }

@Override
protected void addDocs(List<SolrInputDocument> list) throws SolrServerException, IOException {
    _server.add(list, COMMIT_WITHIN);
}

@Override
protected void addChild(SolrInputDocument doc, SolrInputDocument child) {
	_docList.add( child );
}
}