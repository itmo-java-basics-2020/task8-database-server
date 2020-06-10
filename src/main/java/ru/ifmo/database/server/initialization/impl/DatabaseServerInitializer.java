package ru.ifmo.database.server.initialization.impl;

import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.index.impl.TableIndex;
import ru.ifmo.database.server.initialization.InitializationContext;
import ru.ifmo.database.server.initialization.Initializer;
import ru.ifmo.database.server.logic.impl.DatabaseImpl;

import java.io.File;

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
        File dir = new File(context.executionEnvironment().getWorkingPath().toString());
        if (dir.listFiles() == null) {
            throw new DatabaseException("Not correct Env directory path");
        }
        //noinspection ConstantConditions
        for (File database : dir.listFiles(File::isDirectory)) {
            InitializationContext initializationContext = InitializationContextImpl.builder()
                    .executionEnvironment(context.executionEnvironment())
                    .currentDatabaseContext(new DatabaseInitializationContextImpl(database.getName(), database.toPath()))
                    .build();
            databaseInitializer.perform(initializationContext);
        }
    }
}