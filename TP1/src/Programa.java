import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MapReduceCommand;
import com.mongodb.MapReduceOutput;
import com.mongodb.MongoClient;

public class Programa {

	public static void main(String[] args) throws IOException {

		
		FileReader file = new FileReader("C:\\Users\\Yo\\Desktop\\donQuijote.txt");
	
	//	FileReader file = new FileReader("C:\\Users\\Yo\\Desktop\\Movimientos.txt");

        BufferedReader br = new BufferedReader(file);
        
        MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
        
        DB db = mongoClient.getDB("test");
        
        DBCollection coll = db.getCollection("texto");
        
        coll.drop();
     
        String line = br.readLine();
        
        BasicDBObject document ;
		
		while (line != null ) {    
       
            String[] partes = line.split(" ");
            
            for (int i=0; i < partes.length ; i++) {
            	
            	document= new BasicDBObject();

            	if (line.trim().isEmpty() != true){
            		
            		document.put("palabra", partes[i]);
           
            		coll.insert(document);
            	
            	}
            	
            }

            line = br.readLine();   

		}
		
		
		
		String map = "function () {"+
				"emit(this.palabra.length, 1);"+
				"}";
		
		String reduce = "function(cantidad, total) { " +
				"return Array.sum(total);} ";
		
		MapReduceCommand cmd = new MapReduceCommand(coll, map, reduce,
                null, MapReduceCommand.OutputType.INLINE, null);


		MapReduceOutput out = coll.mapReduce(cmd);
				
		
		for (DBObject o : out.results()) {
			
			System.out.println(o.toString());
			
		}
		
		System.out.println("Emit: " + out.getEmitCount());
		
		System.out.println("Output: " + out.getOutputCount());
		
	}

}
