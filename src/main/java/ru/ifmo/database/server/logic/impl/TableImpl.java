package ru.ifmo.database.server.logic.impl;

import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.index.impl.TableIndex;
import ru.ifmo.database.server.initialization.TableInitializationContext;
import ru.ifmo.database.server.logic.Segment;
import ru.ifmo.database.server.logic.Table;

import java.io.File;
import java.nio.file.Path;

/**
 * Таблица - логическая сущность, представляющая собой набор файлов-сегментов, которые объединены одним
 * именем и используются для хранения однотипных данных (данных, представляющих собой одну и ту же сущность,
 * например, таблица "Пользователи")
 * <p>
 * - имеет единый размер сегмента
 * - представляет из себя директорию в файловой системе, именованную как таблица
 * и хранящую файлы-сегменты данной таблицы
 */
public class TableImpl implements Table {
    private final String name;
    private final TableIndex tableIndex;
    private final Path

    static Table create(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) throws DatabaseException {
        try {
            new File(pathToDatabaseRoot + tableName).mkdir();
        }
        catch (Exception e) {
            throw new DatabaseException(e);
        }

    }

    public static Table initializeFromContext(TableInitializationContext context) {
        throw new UnsupportedOperationException(); // todo implement
    }

    @Override
    public String getName() {
        throw new UnsupportedOperationException(); // todo implement
    }

    @Override
    public void write(String objectKey, String objectValue) throws DatabaseException {
        throw new UnsupportedOperationException(); // todo implement
    }

    @Override
    public String read(String objectKey) throws DatabaseException {
        throw new UnsupportedOperationException(); // todo implement
    }
}