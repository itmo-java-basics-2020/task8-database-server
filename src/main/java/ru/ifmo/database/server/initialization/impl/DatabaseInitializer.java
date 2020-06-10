package ru.ifmo.database.server.initialization.impl;

import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.index.impl.SegmentIndex;
import ru.ifmo.database.server.index.impl.TableIndex;
import ru.ifmo.database.server.initialization.InitializationContext;
import ru.ifmo.database.server.initialization.Initializer;
import ru.ifmo.database.server.logic.impl.DatabaseImpl;

import java.io.File;

public class DatabaseInitializer implements Initializer {
    private final Initializer tableInitializer;

    public DatabaseInitializer(Initializer tableInitializer) {
        this.tableInitializer = tableInitializer;
    }

    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        if (context.currentDbContext() == null) {
            throw new DatabaseException("Context Db is null");
        }
        File dir = context.currentDbContext().getDatabasePath().toFile();

        if (dir.listFiles() == null) {
            return;
        }

        File[] tables = dir.listFiles();
        for (File table : tables) {
            InitializationContext init = new InitializationContextImpl(context.executionEnvironment(),
                    context.currentDbContext(),
                    new TableInitializationContextImpl(table.getName(), table.toPath(), new TableIndex()),
                    context.currentSegmentContext());

            tableInitializer.perform(init);
        }

        context.executionEnvironment().addDatabase(DatabaseImpl.initializeFromContext(context.currentDbContext()));
    }
}