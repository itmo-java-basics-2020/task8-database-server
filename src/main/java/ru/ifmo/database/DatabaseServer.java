package ru.ifmo.database;

import ru.ifmo.database.server.console.DatabaseCommand;
import ru.ifmo.database.server.console.DatabaseCommandResult;
import ru.ifmo.database.server.console.DatabaseCommands;
import ru.ifmo.database.server.console.ExecutionEnvironment;
import ru.ifmo.database.server.console.impl.ExecutionEnvironmentImpl;
import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.initialization.Initializer;
import com.ifmo.database.server.initialization.impl.DatabaseInitializer;
import com.ifmo.database.server.initialization.impl.DatabaseServerInitializer;
import com.ifmo.database.server.initialization.impl.InitializationContextImpl;
import com.ifmo.database.server.initialization.impl.SegmentInitializer;
import com.ifmo.database.server.initialization.impl.TableInitializer;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DatabaseServer {

    private static ExecutorService executor = Executors.newSingleThreadExecutor();

    private final ServerSocket serverSocket;
    private final ExecutionEnvironment env;

    public DatabaseServer(ExecutionEnvironment env, Initializer initializer) throws IOException, DatabaseException {
        this.serverSocket = new ServerSocket(4321);
        this.env = env;

        InitializationContextImpl initializationContext = InitializationContextImpl.builder()
                .executionEnvironment(env)
                .build();

        initializer.perform(initializationContext);
    }

    public static void main(String[] args) throws IOException, DatabaseException {

        Initializer initializer = new DatabaseServerInitializer(
                new DatabaseInitializer(new TableInitializer(new SegmentInitializer())));

        DatabaseServer databaseServer = new DatabaseServer(new ExecutionEnvironmentImpl(), initializer);
    }

    public DatabaseCommandResult executeNextCommand(String commandText) {
        try {
            if (StringUtils.isEmpty(commandText)) {
                return DatabaseCommandResult.error("Command name is not specified");
            }

            final String[] args = commandText.split(" ");
            if (args.length < 1) {
                return DatabaseCommandResult.error("Command name is not specified");
            }

            return DatabaseCommands.valueOf(args[0]).getCommand(env, args).execute();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return DatabaseCommandResult.error(e);
        }
    }

    public DatabaseCommandResult executeNextCommand(DatabaseCommand command) {
        try {
            return command.execute();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return DatabaseCommandResult.error(e);
        }
    }
}
