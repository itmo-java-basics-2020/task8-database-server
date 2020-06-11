package ru.ifmo.database.server.initialization.impl;

import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.initialization.DatabaseInitializationContext;
import ru.ifmo.database.server.initialization.InitializationContext;
import ru.ifmo.database.server.initialization.Initializer;

import java.io.File;
import java.io.FilenameFilter;
import java.nio.file.Path;

public class DatabaseServerInitializer implements Initializer {

    private final Initializer databaseInitializer;

    public DatabaseServerInitializer(Initializer databaseInitializer) {
        this.databaseInitializer = databaseInitializer;
    }


    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        File file = new File(context.executionEnvironment().getWorkingPath().toString());
        String[] directories = file.list((current, name) -> new File(current, name).isDirectory());
        if (directories == null) {
            return;
        }
        for (String directory : directories) {
            InitializationContextImpl initializationContext = InitializationContextImpl.builder()
                    .executionEnvironment(context.executionEnvironment())
                    .currentDatabaseContext(new DatabaseInitializationContextImpl(directory,
                            Path.of(context.executionEnvironment().getWorkingPath().toString() + "\\" + directory)))
                    .build();
            databaseInitializer.perform(initializationContext);
        }
    }
}