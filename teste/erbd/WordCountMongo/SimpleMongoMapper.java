package erbd.WordCountMongo;

import java.util.List;

import org.apache.storm.mongodb.common.MongoUtils;
import org.apache.storm.mongodb.common.mapper.MongoMapper;
import org.apache.storm.tuple.ITuple;
import org.bson.Document;

public class SimpleMongoMapper implements MongoMapper {
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String[] fields;

    @Override
    public Document toDocument(ITuple tuple) {
        Document document = new Document();
       
        for(String field : fields){
            document.append(field, tuple.getValueByField(field));
            //System.out.println("teste............."+ tuple.getValueByField(field));
        }
        return document;
    }

    public Document toDocumentByKeys(List<Object> keys) {
        Document document = new Document();
        document.append("_id", MongoUtils.getId(keys));
        return document;
    }

    public SimpleMongoMapper withFields(String... fields) {
        this.fields = fields;
        return this;
    }


}
