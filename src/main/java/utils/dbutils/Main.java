package utils.dbutils;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.io.*;
import java.util.Hashtable;
import java.util.Map;
import java.time.LocalDateTime;

public class Main {

    private static String dbName    = "", oldDbName = "";
    private static int numOfCommands        = 0;
    private static int numOfAllEntries      = 0;
    private static Hashtable<String, PrintStream> outputFiles = new Hashtable<String,PrintStream>();

    public static void printParameters() {
        if (Config.inputFile != null)
            System.err.println("Input log file                            : "+Config.inputFile);
        else
            System.err.println("Input set to StdIn.");
        if (Config.outputDir != null)
            System.err.println("Output directory                          : "+Config.outputDir);
        else
            System.err.println("Output set to StdOut.");
        System.err.println("Commands logging enabled                  : "+Config.commandsLogging);
        if (Config.traceAllDbs())
            System.err.println("All databases are traced.");
        else if (Config.traceInclDbs())
            System.err.println("List of traced databases                  : "+Config.includeDbs);
        else if (Config.traceExclDbs())
            System.err.println("List of databases, which are not traced   : "+Config.excludeDbs);
        if (Config.traceAllCmds())
            System.err.println("All commands are traced.");
        else if (Config.traceInclCmds())
            System.err.println("List of traced commands                   : "+Config.includeCmds);
        else if (Config.traceExclCmds())
            System.err.println("List of commands, which are not traced    : "+Config.excludeCmds);
        switch (Config.executionTracing) {
            case 0: System.err.println("Execution plan tracing disabled.");
                    break;
            case 1: System.err.println("Execution plan tracing level              : QueryPlanner");
                    break;
            case 2: System.err.println("Execution plan tracing level              : ExecutionStats");
                    break;
            case 3: System.err.println("Execution plan tracing level              : AllPlansExecution");
                    break;
        }
    }

    public static void logCommand(String dbName, JsonObject commandObject) throws Exception {
        PrintStream ps;

        commandObject.remove("$db");
        commandObject.remove("lsid");

        numOfCommands++;

        System.err.println("Found Command #"+numOfCommands+" : "+commandObject);

        if (Config.outputDir != null && !outputFiles.containsKey(dbName) ) {
            ps = new PrintStream(new FileOutputStream(Config.outputDir+File.separator+dbName+".js"),true);
            ps.println("use "+dbName);
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

        if (Config.commandsLogging && Config.executionTracing == 0)
            ps.println("console.log('Executing command : "+commandObject+"')");
        else if (Config.commandsLogging && Config.executionTracing != 0)
            ps.println("console.log('Generating execution plan for : "+commandObject+"')");

        switch (Config.executionTracing) {
            case 0: ps.println("db.runCommand(" + commandObject + ")");
                    break;
            case 1: ps.println("try { db.runCommand({explain : " + commandObject + ", verbosity : 'queryPlanner'}); } catch (e) {console.error(\"Execution plan generation does not support this statement. Details: https://www.mongodb.com/docs/manual/reference/command/explain/\")}");
                    break;
            case 2: ps.println("try { db.runCommand({explain : " + commandObject + ", verbosity : 'executionStats'}); } catch (e) {console.error(\"Execution plan generation does not support this statement. Details: https://www.mongodb.com/docs/manual/reference/command/explain/\")}");
                    break;
            case 3: ps.println("try { db.runCommand({explain : " + commandObject + ", verbosity : 'allPlansExecution'}); } catch (e) {console.error(\"Execution plan generation does not support this statement. Details: https://www.mongodb.com/docs/manual/reference/command/explain/\")}");
        }

        if (Config.outputDir != null)
            ps.println("console.log('\\n\\n\\n')");
    }

    public static void main(String[] args) {
        BufferedReader bfro;
        PrintWriter pw;

        Config.readConfiguration();
        System.err.println(LocalDateTime.now()+" : Starting analysis. ");
        System.err.println(LocalDateTime.now());
        printParameters();

        try {
            if (Config.inputFile != null)
                bfro = new BufferedReader((new FileReader(Config.inputFile)));
            else
                bfro = new BufferedReader(new InputStreamReader(System.in));

            String line;

            while ((line = bfro.readLine()) != null) {
                numOfAllEntries++;
                JsonObject logEntry;
                Gson gson = new Gson();
                logEntry = gson.fromJson(line, JsonObject.class);
                if (logEntry.getAsJsonPrimitive("c").getAsString().equals("COMMAND"))
                    if (logEntry.has("attr")) {
                        JsonObject entryAttr = logEntry.getAsJsonObject("attr");
                        if (entryAttr.has("command")) {
                            if (entryAttr.get("command").isJsonObject()) {
                                JsonObject commandObject = entryAttr.getAsJsonObject("command");
                                dbName = commandObject.getAsJsonPrimitive("$db").getAsString();
                                if (Config.traceDatabase(dbName) && Config.traceCommand(commandObject)) {
                                    if ((commandObject.has("documents") && commandObject.get("documents").isJsonArray())||
                                        (!commandObject.has("documents"))) {
                                        logCommand(dbName,commandObject);
                                    }
                                }
                            }
                        }
                    }
            }
            bfro.close();
            for (Map.Entry<String, PrintStream> e : outputFiles.entrySet())
                e.getValue().close();
            System.err.println(LocalDateTime.now()+" : Analysis completed.");
            System.err.println("Summary");
            printParameters();
            System.err.println("Number of entries interpreted as traced commands : "+numOfCommands);
            System.err.println("Number of all entries                            : "+numOfAllEntries);
        } catch (Exception e)
        {e.printStackTrace();}
    }
}