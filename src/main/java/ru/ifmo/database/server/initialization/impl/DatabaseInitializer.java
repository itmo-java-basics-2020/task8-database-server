package ru.ifmo.database.server.initialization.impl;

import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.index.impl.TableIndex;
import ru.ifmo.database.server.initialization.InitializationContext;
import ru.ifmo.database.server.initialization.Initializer;
import ru.ifmo.database.server.logic.impl.DatabaseImpl;

import java.io.File;
import java.nio.file.Path;

public class DatabaseInitializer implements Initializer {
    private final Initializer tableInitializer;

    public DatabaseInitializer(Initializer tableInitializer) {
        this.tableInitializer = tableInitializer;
    }

    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        File file = new File(context.currentDbContext().getDatabasePath().toString());
        String[] directories = file.list((current, name) -> new File(current, name).isDirectory());
        if (directories == null) {
            return;
        }
        for (String directory : directories) {
            InitializationContextImpl initializationContext = InitializationContextImpl.builder()
                    .executionEnvironment(context.executionEnvironment())
                    .currentDatabaseContext(context.currentDbContext())
                    .currentTableContext(new TableInitializationContextImpl(directory,
                            context.currentDbContext().getDatabasePath(), new TableIndex()))
                    .build();
            tableInitializer.perform(initializationContext);
        }
        context.executionEnvironment().addDatabase(DatabaseImpl.initializeFromContext(context.currentDbContext()));
    }
}