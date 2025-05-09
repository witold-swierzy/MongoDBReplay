package utils.dbutils;

import com.google.gson.*;

import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;

public class Config {
    public static String inputFile;
    public static String outputDir;
    public static JsonArray includeDbs;
    public static JsonArray excludeDbs;
    public static JsonArray includeCmds;
    public static JsonArray excludeCmds;
    public static boolean commandsLogging;
    public static int executionTracing = 0; // 0 - not tracing
                                            // 1 - query planner
                                            // 2 - execution stats
                                            // 3 - allPlansExecution
    public static boolean traceAllDbs() {
        return (includeDbs == null && excludeDbs == null);
    }

    public static boolean traceInclDbs() {
        return (includeDbs != null);
    }

    public static boolean traceExclDbs() {
        return (excludeDbs != null);
    }

    public static boolean traceInclCmds() {
        return (includeCmds != null);
    }

    public static boolean traceExclCmds() {
        return (excludeCmds != null);
    }

    public static boolean traceAllCmds() {
        return (includeCmds == null && excludeCmds == null);
    }

    public static boolean traceDatabase(String dbName) {
        boolean contains = false;
        if ( includeDbs == null && excludeDbs == null )
            contains = true;
        else if ( excludeDbs == null )
            contains = includeDbs.contains(new JsonParser().parse(dbName));
        else contains = !excludeDbs.contains(new JsonParser().parse(dbName));
        return contains;
    }

    public static boolean traceCommand(JsonObject command) {
        boolean contains = false;
        int i = 0;
        if (includeCmds == null && excludeCmds == null)
            contains = true;
        else if (excludeCmds == null)
                while (!contains && i < includeCmds.asList().toArray().length) {
                    if (command.has(includeCmds.asList().get(i).getAsString()))
                        contains = true;
                    i++;
                }
        else if (includeCmds == null) {
            contains = true;
            while (contains && i < excludeCmds.asList().toArray().length) {
                if (command.has(excludeCmds.asList().get(i).getAsString()))
                    contains = false;
                i++;
            }
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
            if (configObject.has("INCLUDE_DATABASES") &&
                    configObject.has("EXCLUDE_DATABASES"))
                throw new Exception("You cannot set INCLUDE_DATATABASES and EXCLUDE_DATABASES at the same time. Please, review the available documentation.");

            if (configObject.has("INCLUDE_COMMANDS") &&
                configObject.has("EXCLUDE_COMMANDS"))
                throw new Exception("You cannot set INCLUDE_COMMANDS and EXCLUDE_COMMANDS at the same time. Please, review the available documentation.");

            inputFile = configObject.getAsJsonPrimitive("INPUT_FILE").getAsString();
            outputDir = configObject.getAsJsonPrimitive("OUTPUT_DIR").getAsString();
            if (configObject.has("INCLUDE_DATABASES"))
                includeDbs = configObject.getAsJsonArray("INCLUDE_DATABASES");
            if (configObject.has("EXCLUDE_DATABASES"))
                excludeDbs = configObject.getAsJsonArray("EXCLUDE_DATABASES");

            if (configObject.has("INCLUDE_COMMANDS"))
                includeCmds = configObject.getAsJsonArray("INCLUDE_COMMANDS");

            if (configObject.has("EXCLUDE_COMMANDS"))
                excludeCmds = configObject.getAsJsonArray("EXCLUDE_COMMANDS");

            if (configObject.has("COMMANDS_LOGGING"))
                commandsLogging = configObject.getAsJsonPrimitive("COMMANDS_LOGGING").getAsBoolean();
            else
                commandsLogging = true;

            if (configObject.has("EXECUTION_PLAN_TRACING")) {
                executionTracing = configObject.getAsJsonPrimitive("EXECUTION_PLAN_TRACING").getAsInt();
            }

            reader.close();
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }
    }
}
