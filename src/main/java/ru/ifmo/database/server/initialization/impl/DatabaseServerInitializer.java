package ru.ifmo.database.server.initialization.impl;

import ru.ifmo.database.server.console.ExecutionEnvironment;
import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.initialization.DatabaseInitializationContext;
import ru.ifmo.database.server.initialization.InitializationContext;
import ru.ifmo.database.server.initialization.Initializer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DatabaseServerInitializer implements Initializer {

    private final Initializer databaseInitializer;

    public DatabaseServerInitializer(Initializer databaseInitializer) {
        this.databaseInitializer = databaseInitializer;
    }

    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        ExecutionEnvironment env = context.executionEnvironment();
        Path workingPath = env.getWorkingPath();

        List<Path> databasePaths;
        try (Stream<Path> walk = Files.walk(workingPath)) {
            databasePaths = walk.filter(Files::isDirectory)
                    .filter(p -> workingPath.equals(p.getParent()))
                    .collect(Collectors.toList());
        } catch (IOException ex) {
            throw new DatabaseException(ex);
        }

        for (Path databasePath : databasePaths) {
            DatabaseInitializationContext databaseContext = new DatabaseInitializationContextImpl(
                    databasePath.getFileName().toString(),
                    databasePath
            );
            InitializationContext initializationContext = InitializationContextImpl.builder()
                    .executionEnvironment(env)
                    .currentDatabaseContext(databaseContext)
                    .build();
            databaseInitializer.perform(initializationContext);
        }
    }
}