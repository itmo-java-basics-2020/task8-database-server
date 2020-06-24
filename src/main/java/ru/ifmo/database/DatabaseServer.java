package ru.ifmo.database;

import ru.ifmo.database.server.console.DatabaseCommand;
import ru.ifmo.database.server.console.DatabaseCommandResult;
import ru.ifmo.database.server.console.DatabaseCommands;
import ru.ifmo.database.server.console.ExecutionEnvironment;
import ru.ifmo.database.server.console.impl.ExecutionEnvironmentImpl;
import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.initialization.InitializationContext;
import ru.ifmo.database.server.initialization.Initializer;
import ru.ifmo.database.server.initialization.impl.*;

import java.util.Arrays;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.net.ServerSocket;

public class DatabaseServer {

    private static ExecutorService executor = Executors.newSingleThreadExecutor();
    private final ExecutionEnvironment env;

    public DatabaseServer(ExecutionEnvironment env, Initializer initializer) throws IOException, DatabaseException {
        this.env = env;

        InitializationContextImpl initializationContext = InitializationContextImpl.builder() // example using lombok @Builder
                .executionEnvironment(env)
                .build();

        initializer.perform(initializationContext);
    }

    public static void main(String[] args) throws IOException, DatabaseException {
   	    Files.createDirectory(Path.of("db_files"));
    }

    public DatabaseCommandResult executeNextCommand(String commandText) {

        if(commandText == null || commandText.isBlank()){
            return DatabaseCommandResult.error("The given command is null");
        }

        String[] Args = commandText.split(" ");
        DatabaseCommands SuitableCommand;
        try {
            SuitableCommand = DatabaseCommands.valueOf(Args[0]);
        }
        catch (IllegalArgumentException exc) {
            return DatabaseCommandResult.error("Such command does not exist");
        }
        DatabaseCommand command = SuitableCommand.getCommand(env, Arrays.copyOfRange(Args, 1, Args.length));

        return tryExecute(command);
    }

    private DatabaseCommandResult tryExecute(DatabaseCommand command)
    {
        try {
            return command.execute();
        }
        catch (IllegalArgumentException | DatabaseException exc){
            return DatabaseCommandResult.error("This command can't be executed");
        }
    }
}
