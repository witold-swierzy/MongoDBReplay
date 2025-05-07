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
            String st;
            int numOfCommands = 0;
            int numOfRejectedEntries = 0;
            while ((st = bfro.readLine()) != null) {
                JsonObject jo;
                Gson gson = new Gson();
                jo = gson.fromJson(st, JsonObject.class);
                if (jo.getAsJsonPrimitive("c").getAsString().equals("COMMAND"))
                    if (jo.has("attr")) {
                        JsonObject attr = jo.getAsJsonObject("attr");
                        System.out.println(attr);
                        if (attr.has("command")) {
                            if (attr.get("command").isJsonObject()) {
                                JsonObject commandObject = attr.getAsJsonObject("command");
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
                                        pw.println("db.runCommand(" + commandObject + ")");
                                    }
                                }
                            }
                        }
                        else
                            numOfRejectedEntries++;
                    }
            }
            bfro.close();
            for (Map.Entry<String, PrintWriter> e : outputFiles.entrySet())
                e.getValue().close();
            System.out.println("Summary : ");
            System.out.println("List of traced databases : "+Config.dbNames);
            System.out.println("Number of entries interpreted as commands : "+numOfCommands);
            System.out.println("Number of rejected entries : "+numOfRejectedEntries);
        } catch (Exception e)
        {e.printStackTrace();}
    }
}