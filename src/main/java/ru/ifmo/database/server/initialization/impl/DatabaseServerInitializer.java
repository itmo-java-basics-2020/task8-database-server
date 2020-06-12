package ru.ifmo.database.server.initialization.impl;

import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.initialization.InitializationContext;
import ru.ifmo.database.server.initialization.Initializer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

public class DatabaseServerInitializer implements Initializer {

    private final Initializer databaseInitializer;

    public DatabaseServerInitializer(Initializer databaseInitializer) {
        this.databaseInitializer = databaseInitializer;
    }


    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        if (context.executionEnvironment() == null) {
            throw new DatabaseException("Environment context equals null");
        }
        Path dir = context.executionEnvironment().getWorkingPath();
        Stream<Path> paths;
        try {
            paths = Files.list(dir);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        paths.forEach(path -> {
            InitializationContext initializationContext = InitializationContextImpl.builder()
                    .executionEnvironment(context.executionEnvironment())
                    .currentDatabaseContext(new DatabaseInitializationContextImpl(path.getFileName().toString(), path.toAbsolutePath()))
                    .build();
            try {
                databaseInitializer.perform(initializationContext);
            } catch (DatabaseException e) {
                e.printStackTrace();
            }
        });
    }
}