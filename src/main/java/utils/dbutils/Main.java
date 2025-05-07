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
    public static void main(String[] args) {
        Config.readConfiguration();
        try {
            BufferedReader bfro = new BufferedReader((new FileReader(Config.inputFile)));
            Hashtable<String, PrintWriter> outputFiles = new Hashtable<String,PrintWriter>();
            String line;
            int numOfCommands = 0;
            int numOfRejectedEntries = 0;
            while ((line = bfro.readLine()) != null) {
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
                                if (Config.containsDbName(dbName)) {
                                    numOfCommands++;
                                    System.out.println("Found Command #"+numOfCommands+" : "+commandObject);
                                    if ((commandObject.has("documents") && commandObject.get("documents").isJsonArray())||
                                        (!commandObject.has("documents"))) {
                                        if (!outputFiles.containsKey(dbName)) {
                                            PrintWriter pw = new PrintWriter(Config.outputDir+File.separator+dbName+".js");
                                            pw.println("use "+dbName);
                                            outputFiles.put(dbName,pw);
                                        }
                                        PrintWriter pw = outputFiles.get(dbName);
                                        if (Config.commandsLogging)
                                            pw.println("console.log('Executing "+commandObject+"')");
                                        pw.println("db.runCommand(" + commandObject + ")");
                                        pw.println("console.log('\\n\\n\\n')");
                                    }
                                }
                            }
                        }
                        else numOfRejectedEntries++;
                    }
            }
            bfro.close();
            for (Map.Entry<String, PrintWriter> e : outputFiles.entrySet())
                e.getValue().close();
            System.out.println("Summary : ");
            System.out.println("Input log file                            : "+Config.inputFile);
            System.out.println("Output directory                          : "+Config.outputDir);
            System.out.println("Commands logging enabled                  : "+Config.commandsLogging);
            System.out.println("List of traced databases                  : "+Config.dbNames);
            System.out.println("Number of entries interpreted as commands : "+numOfCommands);
            System.out.println("Number of rejected entries                : "+numOfRejectedEntries);
        } catch (Exception e)
        {e.printStackTrace();}
    }
}