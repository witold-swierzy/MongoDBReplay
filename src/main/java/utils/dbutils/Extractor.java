package utils.dbutils;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.io.*;
import java.util.Hashtable;
import java.util.Map;
import java.time.LocalDateTime;


public class Extractor {

    private static String dbName    = "", oldDbName = "";
    private static int numOfCommands        = 0;
    private static int numOfAllEntries      = 0;
    private static Hashtable<String, PrintStream> outputFiles = new Hashtable<String,PrintStream>();
    private static BufferedReader inputFile;
    private static PrintStream logFile;
    public static int sem = 0;

    public synchronized static void setSem(int newSem) {
        sem = newSem;
    }

    public static void printSettings(boolean footer) {
        if (!footer)
            logFile.println(LocalDateTime.now()+" : Starting analysis. ");
        else {
            logFile.println(LocalDateTime.now()+" : Analysis completed.");
            logFile.println("Summary");
        }

        if (Config.inputFileName != null)
            logFile.println("Input log file                            : "+Config.inputFileName);
        else
            logFile.println("Input set to StdIn.");
        if (Config.outputDir != null)
            logFile.println("Output directory                          : "+Config.outputDir);
        else
            logFile.println("Output set to StdOut.");
        System.err.println("Commands logging enabled                  : "+Config.commandsLogging);
        if (Config.traceAllDbs())
            logFile.println("All databases are traced.");
        else if (Config.traceInclDbs())
            logFile.println("List of traced databases                  : "+Config.includeDbs);
        else if (Config.traceExclDbs())
            logFile.println("List of databases, which are not traced   : "+Config.excludeDbs);
        if (Config.traceAllCmds())
            logFile.println("All commands are traced.");
        else if (Config.traceInclCmds())
            logFile.println("List of traced commands                   : "+Config.includeCmds);
        else if (Config.traceExclCmds())
            logFile.println("List of commands, which are not traced    : "+Config.excludeCmds);
        switch (Config.executionTracing) {
            case 0: logFile.println("Execution plan tracing disabled.");
                    break;
            case 1: logFile.println("Execution plan tracing level              : QueryPlanner");
                    break;
            case 2: logFile.println("Execution plan tracing level              : ExecutionStats");
                    break;
            case 3: logFile.println("Execution plan tracing level              : AllPlansExecution");
                    break;
        }
        if (footer) {
            logFile.println("Number of entries interpreted as traced commands : "+numOfCommands);
            logFile.println("Number of all entries                            : "+numOfAllEntries);
        }
    }

    public synchronized static void shutdown() {
        try {
            while(sem==1);
            setSem(1);
            inputFile.close();
            for (Map.Entry<String, PrintStream> e : outputFiles.entrySet())
                e.getValue().close();
            printSettings(true);
            System.exit(0);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }
    }

    public static void logCommand(String dbName, JsonObject commandObject) throws Exception {
        PrintStream ps;

        //commandObject.remove("$db");
        commandObject.remove("lsid");

        numOfCommands++;

        logFile.println("Command #"+numOfCommands+" : "+commandObject);

        if (Config.outputDir != null && !outputFiles.containsKey(dbName) ) {
            if (Config.outputMode == 0) {
                ps = new PrintStream(new FileOutputStream(Config.outputDir + File.separator + dbName + ".js"), true);
                ps.println("use " + dbName);
            } else
                ps = new PrintStream(new FileOutputStream(Config.outputDir + File.separator + dbName + ".json"), true);
            outputFiles.put(dbName,ps);
        } else if (Config.outputDir != null )
            ps = outputFiles.get(dbName);
        else {
            ps = System.out;
            if (!oldDbName.equals(dbName)) {
                ps.println("use " + dbName);
                oldDbName = dbName;
            }
        }

        if (Config.outputMode == 0) {
            if (Config.commandsLogging && Config.executionTracing == 0)
                ps.println("console.log('Executing command : " + commandObject + "')");
            else if (Config.commandsLogging && Config.executionTracing != 0)
                ps.println("console.log('Generating execution plan for : " + commandObject + "')");

            switch (Config.executionTracing) {
                case 0:
                    ps.println("db.runCommand(" + commandObject + ")");
                    break;
                case 1:
                    ps.println("try { db.runCommand({explain : " + commandObject + ", verbosity : 'queryPlanner'}); } catch (e) {console.error(\"Execution plan generation does not support this statement. Details: https://www.mongodb.com/docs/manual/reference/command/explain/\")}");
                    break;
                case 2:
                    ps.println("try { db.runCommand({explain : " + commandObject + ", verbosity : 'executionStats'}); } catch (e) {console.error(\"Execution plan generation does not support this statement. Details: https://www.mongodb.com/docs/manual/reference/command/explain/\")}");
                    break;
                case 3:
                    ps.println("try { db.runCommand({explain : " + commandObject + ", verbosity : 'allPlansExecution'}); } catch (e) {console.error(\"Execution plan generation does not support this statement. Details: https://www.mongodb.com/docs/manual/reference/command/explain/\")}");
            }

            if (Config.outputDir != null)
                ps.println("console.log('\\n\\n\\n')");
        }
        else {
            switch (Config.executionTracing) {
                case 0:
                    ps.println(commandObject);
                    break;
                case 1:
                    ps.println("{explain : " + commandObject + ", verbosity : 'queryPlanner'}");
                    break;
                case 2:
                    ps.println("{explain : " + commandObject + ", verbosity : 'executionStats'}");
                    break;
                case 3:
                    ps.println("db.runCommand({explain : " + commandObject + ", verbosity : 'allPlansExecution'}");
            }
        }
    }

    public static void initialize() {
        try {
            Config.readConfiguration(Config.EXTRACT);
            if (Config.inputFileName != null)
                inputFile = new BufferedReader((new FileReader(Config.inputFileName)));
            else
                inputFile = new BufferedReader(new InputStreamReader(System.in));

            if (Config.logFileName != null)
                logFile = new PrintStream(new FileOutputStream(Config.logFileName), true);
            else
                logFile = System.err;
            printSettings(false);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }
    }

    public static void main(String[] args) {
        String line;

        initialize();

        try {
            if (Config.inputFileName != null)
                inputFile = new BufferedReader((new FileReader(Config.inputFileName)));
            else
                inputFile = new BufferedReader(new InputStreamReader(System.in));
            while ((line = inputFile.readLine()) != null) {
                while(sem==1);
                setSem(1);
                numOfAllEntries++;
                JsonObject logEntry;
                Gson gson = new Gson();
                logEntry = gson.fromJson(line, JsonObject.class);
                if (logEntry != null &&
                    logEntry.getAsJsonPrimitive("c").getAsString().equals("COMMAND") &&
                    logEntry.has("attr")) {
                        JsonObject entryAttr = logEntry.getAsJsonObject("attr");
                        if (entryAttr.has("command") && entryAttr.get("command").isJsonObject()) {
                                JsonObject commandObject = entryAttr.getAsJsonObject("command");
                                dbName = commandObject.getAsJsonPrimitive("$db").getAsString();
                                if ((Config.traceDatabase(dbName) &&
                                    Config.traceCommand(commandObject)) &&
                                     ((commandObject.has("documents") &&
                                       commandObject.get("documents").isJsonArray())||
                                      (!commandObject.has("documents"))))
                                            logCommand(dbName,commandObject);
                        }
                }
                setSem(0);
            }
            shutdown();
        } catch (Exception e)
        {e.printStackTrace();}
    }
}