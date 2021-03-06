package GrizzlyExample;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.HashMap;
import java.util.Iterator;

/**
 * Created by martini on 2016-03-11.
 */
@Path("/ByYear/{query}")
public class ByYear {
    private final Table movieTable = DataBase.getMovieTable();

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public String listByYear(@PathParam("query") String param){
        System.out.println("ByYear!");
        int query = Integer.parseInt(param);
        System.out.println("Querying mooovies!");
        String result = "movies from: "+param;


        HashMap<String, String> nameMap = new HashMap<String, String>();
        nameMap.put("#yr", "year");

        HashMap<String, Object> valueMap = new HashMap<String, Object>();
        valueMap.put(":yyyy", query);

        QuerySpec querySpec = new QuerySpec()
                .withKeyConditionExpression("#yr = :yyyy")
                .withNameMap(new NameMap().with("#yr", "year"))
                .withValueMap(valueMap);


        ItemCollection<QueryOutcome> items;
        Iterator<Item> iterator;
        Item item;
        try {
            items = movieTable.query(querySpec);

            iterator = items.iterator();
            while (iterator.hasNext()) {
                item = iterator.next();
                result +="\n"+ item.getNumber("year") + ": "
                        + item.getString("title");
            }

        }catch(Exception e){
            return "Unable to query movies from " + param + " \n" + e.getMessage();
        }



        return result;
    }
}
