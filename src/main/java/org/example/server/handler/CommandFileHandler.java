package org.example.server.handler;

import java.io.*;

public class CommandFileHandler {
    private static final String COMMAND_FOUND_TEXT = "Command Found";
    private static final String PATH_NAME = "commandFile";
    private static final String FILE_NAME = "commands.txt";

    public boolean isCommand(String inputCommand) {
        String filePath = PATH_NAME + File.separator + FILE_NAME;
        try (BufferedReader bf = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = bf.readLine()) != null) {
                if (line.equals(inputCommand)) {
                    System.out.println(COMMAND_FOUND_TEXT);
                    return true;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Can't read file", e);
        }
        return false;
    }
}
