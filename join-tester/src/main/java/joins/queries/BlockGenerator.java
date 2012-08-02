package joins.queries;

public class BlockGenerator extends QueryGenerator {

	public static void main(String[] args) {
		BlockGenerator qg = new BlockGenerator();
	    main(qg);
	  }
	
	@Override
	protected String query(String terms, String aclRange, String place) {
		
		return String.format("q=text_all:(%s) AND _query_:\"{!parent which=kind:body}acl:%s\"" +// {!parent which="PARENT:true"}CHILD_PRICE:10
		  "&fl=id,score&sort=score desc",
		  terms, aclRange, place);
	}
}
