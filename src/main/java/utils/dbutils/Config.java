package utils.dbutils;

import com.google.gson.*;


import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

public class Config {
    public static String inputFile;
    public static String outputDir;
    public static JsonArray dbNames;

    public static boolean containsDbName(String dbName) {
        return  dbNames.contains(new JsonParser().parse(dbName));
    }

    public static void readConfiguration() {
        try {
            Reader reader = Files.newBufferedReader(Paths.get(System.getenv("MR_CONFIG_FILE")));
            JsonObject configObject = (new Gson()).fromJson(reader, JsonObject.class);
            inputFile = configObject.getAsJsonPrimitive("INPUT_FILE").getAsString();
            outputDir = configObject.getAsJsonPrimitive("OUTPUT_DIR").getAsString();
            dbNames = configObject.getAsJsonArray("DB_NAMES");
            reader.close();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
