package GrizzlyExample;


import com.amazonaws.regions.RegionUtils;
import com.amazonaws.regions.ServiceAbbreviations;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;


import java.util.Arrays;
import java.util.Iterator;


/**
 * Created by martini on 2016-03-11.
 */
 final class DataBase {
    static com.amazonaws.regions.Region currentRegion;



   private static AmazonDynamoDBClient client = new AmazonDynamoDBClient()
    .withEndpoint("http://localhost:8000");
    private static DynamoDB dynamoDB = new DynamoDB(client);
    private static String movieTableName = "Movies";


    private static Table movieTable;
    static Table getMovieTable(){
        return movieTable;
    }



    public static String readUrl(String urlString) throws IOException {
        URL url = new URL(urlString);
        BufferedReader reader = null;

        try {
            HttpURLConnection huc = (HttpURLConnection) url.openConnection();
            HttpURLConnection.setFollowRedirects(false);
            huc.setConnectTimeout(100);
            huc.setRequestMethod("GET");
            huc.setRequestProperty("User-Agent", "anders");
            huc.connect();
            InputStream input = huc.getInputStream();

            reader = new BufferedReader(new InputStreamReader(input, "UTF-8"));
            StringBuilder builder = new StringBuilder();
            for (String line; (line = reader.readLine()) != null; ) {
                builder.append(line).append("\n");
            }
            return builder.toString();
        } finally {
            if (reader != null)
                try {
                    reader.close();
                } catch (IOException ignore) {

                }
        }

    }



     static com.amazonaws.regions.Region getCurrentRegion() {
        if (currentRegion == null) {
                try {
                        String zone = readUrl("us-west1a");
                    //zone will be like eu-west1a and then substring -> eu-west, to upper-> EU-WEST and
                    //then replaced -> EU_WEST which corresponds to the enum value
                    currentRegion = RegionUtils.getRegion(
                        zone.substring(0, zone.length() - 2).toUpperCase().replace("-", "_")
                            );
                    return currentRegion;
                } catch (IOException e) {
                    throw new RuntimeException("Server is an ec2 instance but could not get current zone!", e);
                }

        }
        return currentRegion;
    }

    static void Connect(){
        getCurrentRegion().getServiceEndpoint(ServiceAbbreviations.Dynamodb);
    }



     static void CreateTable(){

        try {
            System.out.println("Attempting to create table; please wait...");
            movieTable = dynamoDB.createTable(movieTableName,
                    Arrays.asList(
                            new KeySchemaElement("year", KeyType.HASH),  //Partition key
                            new KeySchemaElement("title", KeyType.RANGE)), //Sort key
                    Arrays.asList(
                            new AttributeDefinition("year", ScalarAttributeType.N),
                            new AttributeDefinition("title", ScalarAttributeType.S)),
                    new ProvisionedThroughput(10L, 10L));
           movieTable.waitForActive();
            System.out.println("Success.  Table status: " + movieTable.getDescription().getTableStatus());


        } catch (Exception e) {
            System.err.println("Unable to create table: ");
            System.err.println(e.getMessage());
        }

        movieTable = dynamoDB.getTable(movieTableName);

    }


    static void importData() throws IOException{

        JsonParser parser = new JsonFactory().createParser(new File("moviedata.json"));
        JsonNode rootNode = new ObjectMapper().readTree(parser);

        Iterator<JsonNode> iter = rootNode.iterator();
        ObjectNode currentNode;

        while (iter.hasNext()) {
            currentNode = (ObjectNode) iter.next();

            int year = currentNode.path("year").asInt();
            String title = currentNode.path("title").asText();

            try {
                movieTable.putItem(new Item()
                        .withPrimaryKey("year", year, "title", title).withJSON("info", currentNode.path("info").toString())
                );
                System.out.println("putitem succeeded: " + year + " " + title);
            } catch (Exception e) {
                System.err.println("unable to add movie: " + year + "" + title);
                System.err.println(e.getMessage());
                break;
            }
        }
        parser.close();
    }
    static void drop() throws InterruptedException {
        Table table = getMovieTable();
        table.delete();
        table.waitForDelete();
        System.out.println("Dropped table");
    }


}
