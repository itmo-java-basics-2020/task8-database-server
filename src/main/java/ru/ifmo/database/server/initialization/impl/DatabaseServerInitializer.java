package ru.ifmo.database.server.initialization.impl;

import ru.ifmo.database.server.console.ExecutionEnvironment;
import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.initialization.InitializationContext;
import ru.ifmo.database.server.initialization.Initializer;

import java.io.File;

public class DatabaseServerInitializer implements Initializer {

    private final Initializer databaseInitializer;

    public DatabaseServerInitializer(Initializer databaseInitializer) {
        this.databaseInitializer = databaseInitializer;
    }


    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        ExecutionEnvironment executionEnvironment = context.executionEnvironment();
        if (executionEnvironment == null) {
            throw new DatabaseException("Execution environment is null");
        }

        File dbServerDir = executionEnvironment.getWorkingPath().toFile();

        if (dbServerDir.listFiles() == null) {
            return;
        }

        for (File directory : dbServerDir.listFiles()) {
            if (directory.isDirectory()) {
                this.databaseInitializer.perform(InitializationContextImpl.builder()
                        .executionEnvironment(executionEnvironment)
                        .currentDatabaseContext(
                                new DatabaseInitializationContextImpl(
                                        directory.getName(),
                                        directory.toPath()))
                        .build());
            }
        }
    }
}
