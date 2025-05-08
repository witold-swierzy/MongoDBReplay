package utils.dbutils;

import com.google.gson.*;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;

public class Config {
    public static String inputFile;
    public static String outputDir;
    public static JsonArray dbNames;
    public static JsonArray commands;
    public static boolean allCommands = false;
    public static boolean commandsLogging;

    public static boolean containsDbName(String dbName) {
        return  dbNames.contains(new JsonParser().parse(dbName));
    }

    public static boolean containsCommand(JsonObject command) {
        boolean contains = false;
        int i = 0;
        if (allCommands)
            contains = true;
        else while (!contains && i < commands.asList().toArray().length){
                if (command.has(commands.asList().get(i).getAsString()))
                    contains = true;
                i++;
            }
        return contains;
    }

    public static void readConfiguration() {
        try {
            String configFileName = System.getenv("MR_CONFIG_FILE");
            if ( configFileName == null || configFileName.length() ==0 )
                throw new Exception("MR_CONFIG_FILE environment variable is mandatory. Please review the available documentation.");

            Reader reader = Files.newBufferedReader(Paths.get(configFileName));
            JsonObject configObject = (new Gson()).fromJson(reader, JsonObject.class);

            if (!configObject.has("INPUT_FILE"))
                throw new Exception("INPUT_FILE paramter is mandatory. Please, review the available documentation.");
            if (!configObject.has("OUTPUT_DIR"))
                throw new Exception("OUPTUT_DIR parameter is mandatory. Please, review the available documentation");
            if (!configObject.has("DB_NAMES"))
                throw new Exception("DB_NAMES parameter is mandatory. Please, review the available documentation.");

            inputFile = configObject.getAsJsonPrimitive("INPUT_FILE").getAsString();
            outputDir = configObject.getAsJsonPrimitive("OUTPUT_DIR").getAsString();
            dbNames = configObject.getAsJsonArray("DB_NAMES");

            if (configObject.has("COMMANDS"))
                commands = configObject.getAsJsonArray("COMMANDS");
            else
                allCommands = true;

            if (configObject.has("COMMANDS_LOGGING"))
                commandsLogging = configObject.getAsJsonPrimitive("COMMANDS_LOGGING").getAsBoolean();
            else
                commandsLogging = true;

            reader.close();
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }
    }
}
