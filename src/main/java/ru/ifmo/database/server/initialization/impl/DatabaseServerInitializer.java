package ru.ifmo.database.server.initialization.impl;

import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.initialization.InitializationContext;
import ru.ifmo.database.server.initialization.Initializer;

import java.io.File;
import java.util.Objects;

public class DatabaseServerInitializer implements Initializer {

    private final Initializer databaseInitializer;

    public DatabaseServerInitializer(Initializer databaseInitializer) {
        this.databaseInitializer = databaseInitializer;
    }

    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        if (context.executionEnvironment() == null) {
            throw new DatabaseException("Execution environment must not be null");
        }

        File databaseServerDir = context.executionEnvironment().getWorkingPath().toFile();
        if (databaseServerDir.listFiles() == null) {
            return;
        }

        for (File databaseDir : Objects.requireNonNull(databaseServerDir.listFiles())) {
            if (!databaseDir.isDirectory()) {
                continue;
            }

            databaseInitializer.perform(InitializationContextImpl.builder()
                    .executionEnvironment(context.executionEnvironment())
                    .currentDatabaseContext(new DatabaseInitializationContextImpl(
                            databaseDir.getName(), databaseDir.toPath()
                    ))
                    .build());
        }
    }
}