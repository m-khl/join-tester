package joins.indexer;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Collection;
import java.util.List;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;

public class CSVIndexer extends ProtoIndexer {

    private Writer parents;
    private Writer children;
    private boolean[] parentsHeader = new boolean[]{false};
    private boolean[] childrenHeader = new boolean[]{false};

    public CSVIndexer() throws IOException {
        parents = new OutputStreamWriter(new BufferedOutputStream(new FileOutputStream("parent.csv", false)));
        children = new OutputStreamWriter(new BufferedOutputStream(new FileOutputStream("children.csv", false)));
    }
    
    public static void main(String[] args) throws Exception {
        main(new CSVIndexer());
    }
    
    @Override
    protected void addDocs(List<SolrInputDocument> list)
            throws SolrServerException, IOException {
        for(SolrInputDocument doc: list){
            dump(parents, doc, parentsHeader);
        }
    }

    protected void dump(final Writer out, SolrInputDocument doc,
            final boolean[] header2) throws IOException {
        if(!header2[0]){
            boolean first = true;
            for(String fname:doc.getFieldNames()){
                if(!first){
                    out.append(", ");
                }
                first= false;
                out.append(fname);
            }
            out.append("\n");
            header2[0] = true;
        }
        boolean first = true;
        for(String fname:doc.getFieldNames()){
            if(!first){
                out.append(", ");
            }
            first= false;
            final Collection<Object> vals = doc.getFieldValues(fname);
            if(vals.size()>1){
                throw new IllegalArgumentException(fname+ " = "+vals);
            }
            out.append(""+vals.iterator().next());
        }
        out.append("\n");
    }

    @Override
    protected void addChild(SolrInputDocument doc, SolrInputDocument child) throws IOException {
        dump(children, child, childrenHeader);
    }
    
    @Override
    void generateAllDocs() throws Exception {
       generateDocs(10);
       flushTail();
       parents.flush();
       parents.close();
       children.flush();
       children.close();
    }

}
