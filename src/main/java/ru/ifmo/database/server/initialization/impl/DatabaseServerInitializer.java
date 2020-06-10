package ru.ifmo.database.server.initialization.impl;

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
        if (context.executionEnvironment() == null) {
            throw new DatabaseException("Context Env is null");
        }

        File dir = context.executionEnvironment().getWorkingPath().toFile();

        if (dir.listFiles() == null) {
            return;
        }

        File[] files = dir.listFiles();
        for (File file : files) {
            if (file.isDirectory()) {
                InitializationContext init = InitializationContextImpl.builder()
                        .executionEnvironment(context.executionEnvironment())
                        .databaseInitializationContext(new DatabaseInitializationContextImpl(file.getName(), file.toPath())).build();

                databaseInitializer.perform(init);
            }
        }
    }
}