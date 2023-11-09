package org.example.server.handler;

public class ClientRequestHandler {
    private static final CommandFileHandler fileHandler = new CommandFileHandler();
    private static final String COMMAND_TEXT = "This is command: ";
    private static final String SPACE = " ";
    private static final String UNDERSCORE = "_";
    private static final String INVALID_TEXT_RESPONSE = "Invalid input";
    private static final int COEFFICIENT = 1000;

    public String processInput(String input) {
        if (fileHandler.isCommand(input)) {
            return COMMAND_TEXT + input;
        }
        String s = input.replaceAll(SPACE, UNDERSCORE);
        if (s.matches("\\d+")) {
            int number = Integer.parseInt(s);
            return String.valueOf(number * COEFFICIENT);
        } else if (s.matches("[a-zA-Z]+") || s.contains(UNDERSCORE)) {
            return alternateCase(s);
        } else {
            return INVALID_TEXT_RESPONSE;
        }
    }

    private String alternateCase(String input) {
        StringBuilder result = new StringBuilder();
        boolean toUpperCase = true;

        for (char c : input.toCharArray()) {
            if (Character.isLetter(c)) {
                if (toUpperCase) {
                    result.append(Character.toUpperCase(c));
                } else {
                    result.append(Character.toLowerCase(c));
                }
                toUpperCase = !toUpperCase;
            } else {
                result.append(c);
            }
        }

        return result.toString();
    }
}