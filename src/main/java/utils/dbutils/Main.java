package utils.dbutils;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.Hashtable;
import java.util.Map;

public class Main {

    public static void printParameters() {
        System.out.println("Input log file                            : "+Config.inputFile);
        System.out.println("Output directory                          : "+Config.outputDir);
        System.out.println("Commands logging enabled                  : "+Config.commandsLogging);
        if (Config.traceAllDbs())
            System.out.println("All databases are traced.");
        else if (Config.traceInclDbs())
            System.out.println("List of traced databases                  : "+Config.includeDbs);
        else if (Config.traceExclDbs())
            System.out.println("List of databases, which are not traced   : "+Config.excludeDbs);
        if (Config.traceAllCmds())
            System.out.println("All commands are traced.");
        else if (Config.traceInclCmds())
            System.out.println("List of traced commands                   : "+Config.includeCmds);
        else if (Config.traceExclCmds())
            System.out.println("List of commands, which are not traced    : "+Config.excludeCmds);
        switch (Config.executionTracing) {
            case 0: System.out.println("Execution plan tracing disabled.");
                    break;
            case 1: System.out.println("Execution plan tracing level              : QueryPlanner");
                    break;
            case 2: System.out.println("Execution plan tracing level              : ExecutionStats");
                    break;
            case 3: System.out.println("Execution plan tracing level              : AllPlansExecution");
                    break;
        }
    }

    public static void main(String[] args) {
        Config.readConfiguration();
        System.out.println("Starting analysis. ");
        printParameters();

        try {
            BufferedReader bfro = new BufferedReader((new FileReader(Config.inputFile)));
            Hashtable<String, PrintWriter> outputFiles = new Hashtable<String,PrintWriter>();
            String line;
            int numOfCommands        = 0;
            int numOfAllEntries      = 0;
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
                                String dbName = commandObject.getAsJsonPrimitive("$db").getAsString();
                                if (Config.traceDatabase(dbName) && Config.traceCommand(commandObject)) {
                                    numOfCommands++;
                                    System.out.println("Found Command #"+numOfCommands+" : "+commandObject);
                                    if ((commandObject.has("documents") && commandObject.get("documents").isJsonArray())||
                                        (!commandObject.has("documents"))) {
                                        if (!outputFiles.containsKey(dbName)) {
                                            PrintWriter pw = new PrintWriter(Config.outputDir+File.separator+dbName+".js");
                                            pw.println("use "+dbName);
                                            outputFiles.put(dbName,pw);
                                        }
                                        commandObject.remove("$db");
                                        commandObject.remove("lsid");
                                        PrintWriter pw = outputFiles.get(dbName);
                                        if (Config.commandsLogging && Config.executionTracing == 0)
                                            pw.println("console.log('Executing "+commandObject+"')");
                                        else if (Config.executionTracing != 0)
                                            pw.println("console.log('Checking execution plan of "+commandObject+"')");
                                        switch (Config.executionTracing) {
                                            case 0:
                                                pw.println("db.runCommand(" + commandObject + ")");
                                                break;
                                            case 1:
                                                pw.println("try { db.runCommand({explain : " + commandObject + ", verbosity : 'queryPlanner'}); } catch (e) {console.error(\"Execution plan generation does not support this statement. Details: https://www.mongodb.com/docs/manual/reference/command/explain/\")}");
                                                break;
                                            case 2:
                                                pw.println("try { db.runCommand({explain : " + commandObject + ", verbosity : 'executionStats'}); } catch (e) {console.error(\"Execution plan generation does not support this statement. Details: https://www.mongodb.com/docs/manual/reference/command/explain/\")}");
                                                break;
                                            case 3:
                                                pw.println("try { db.runCommand({explain : " + commandObject + ", verbosity : 'allPlansExecution'}); } catch (e) {console.error(\"Execution plan generation does not support this statement. Details: https://www.mongodb.com/docs/manual/reference/command/explain/\")}");
                                        }
                                        pw.println("console.log('\\n\\n\\n')");
                                    }
                                }
                            }
                        }
                    }
            }
            bfro.close();
            for (Map.Entry<String, PrintWriter> e : outputFiles.entrySet())
                e.getValue().close();
            System.out.println("Analysis completed.");
            System.out.println("Summary");
            printParameters();
            System.out.println("Number of entries interpreted as traced commands : "+numOfCommands);
            System.out.println("Number of all entries                            : "+numOfAllEntries);
        } catch (Exception e)
        {e.printStackTrace();}
    }
}