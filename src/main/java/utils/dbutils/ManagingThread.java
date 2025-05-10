package utils.dbutils;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;

public class ManagingThread extends Thread {
    public void run() {
        PrintStream ps1;
        try {
            ps1 = new PrintStream(new FileOutputStream(Config.logFileName),true);
            while (true) {
                Thread.sleep(1000);
                if (Files.exists(Paths.get(Config.shutdownFileName))) {
                    Files.delete(Paths.get(Config.shutdownFileName));
                    ps1.println("Shutting down MongoDB Replay instance.");
                    ps1.close();
                    Main.shutdown();
                }
            }
        }
        catch (Exception e) {}
    }
}
