package GrizzlyExample;

import com.amazonaws.services.dynamodbv2.document.Table;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.io.IOException;

/**
 * Created by martini on 2016-03-14.
 */
@Path("drop")
public class drop {

        private Table table = DataBase.getMovieTable();


        @GET
        @Produces(MediaType.TEXT_PLAIN)
        public String drop(){
            try {
                DataBase.drop();
                return "success!";
            } catch ( InterruptedException e) {
                e.printStackTrace();
                return "failed";
            }
        }
}
