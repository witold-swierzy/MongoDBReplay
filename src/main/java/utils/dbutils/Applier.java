package utils.dbutils;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonDocument;
import org.bson.Document;

import java.io.*;
import java.time.LocalDateTime;

public class Applier {
    private static int numOfSucceses     = 0;
    private static int numOfErrors       = 0;
    private static int numOfAllCommands  = 0;
    private static String dbName, oldDbname;
    private static PrintStream logFile;
    private static MongoClient client;
    private static MongoDatabase db;
    private static BufferedReader inputFile;

    public static void printSettings(boolean footer) {
        if (!footer)
            logFile.println(LocalDateTime.now() + " : Starting commands application. ");
        else {
            logFile.println(LocalDateTime.now() + " : Commands application completed.");
            logFile.println("Summary");
        }

        if (Config.inputFileName != null)
            logFile.println("Input file              : " + Config.inputFileName);
        else
            logFile.println("Input redirected to StdIn");

        if (Config.logFileName != null)
            logFile.println("Log file                : " + Config.logFileName);
        else
            logFile.println("Logging set to StdOut.");
        System.out.println("Database connect string : "+Config.connectString);
        if (footer)
        {
            logFile.println("Number of all commands          : " + numOfAllCommands);
            logFile.println("Number of successful executions : " + numOfSucceses);
            logFile.println("Number of errors                : " + numOfErrors);
        }
    }

    public static void initialize() {
        try {
            Config.readConfiguration(Config.APPLY);
            if (Config.logFileName != null)
                logFile = new PrintStream(new FileOutputStream(Config.logFileName), true);
            else
                logFile = System.err;
            printSettings(false);
            client = MongoClients.create(new ConnectionString(Config.connectString));
            db     = client.getDatabase(Config.dbName);
            if (Config.inputFileName != null)
                inputFile = new BufferedReader((new FileReader(Config.inputFileName)));
            else
                inputFile = new BufferedReader(new InputStreamReader(System.in));
            dbName = Config.dbName;
            oldDbname = Config.dbName;
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }
    }

    public static synchronized void shutdown() {
            try {
                printSettings(true);
                logFile.close();
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(0);
            }
    }

    public static void main(String[] args) {
        String line;
        JsonObject commandJSON;
        BsonDocument commandBSON;
        Document commandResult;

        initialize();
        try {
            while ((line = inputFile.readLine()) != null) {
                numOfAllCommands++;
                try {
                    logFile.println("Command #"+numOfAllCommands+" : "+line);
                    Gson gson = new Gson();
                    commandJSON = gson.fromJson(line, JsonObject.class);
                    dbName = commandJSON.getAsJsonPrimitive("$db").getAsString();
                    if (!dbName.equals(oldDbname)) {
                        db = client.getDatabase(dbName);
                        oldDbname = dbName;
                    }
                    commandJSON.remove("$db");
                    commandBSON = BsonDocument.parse(commandJSON.toString());
                    commandResult = db.runCommand(commandBSON);
                    logFile.println("Result : "+commandResult.toString().substring(8));
                    numOfSucceses++;
                } catch(Exception e) {
                    numOfErrors++;
                    logFile.println(e);
                }
            }
        } catch (Exception e)
        {e.printStackTrace();}
        shutdown();
    }
}
