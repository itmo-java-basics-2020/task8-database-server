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
        File dir = context.executionEnvironment().getWorkingPath().toFile();

        File[] files = dir.listFiles();
        for (File file : files) {
            InitializationContext init = new InitializationContextImpl(context.executionEnvironment(),
                    new DatabaseInitializationContextImpl(file.getName(), file.toPath()),
                    context.currentTableContext(),
                    context.currentSegmentContext());

            databaseInitializer.perform(init);
        }
    }
}