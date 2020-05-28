package ru.ifmo.database.server.logic.impl;

import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.index.impl.TableIndex;
import ru.ifmo.database.server.initialization.TableInitializationContext;
import ru.ifmo.database.server.logic.Segment;
import ru.ifmo.database.server.logic.Table;

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
    private final String tableName;
    private final Path tablePath;
    private final TableIndex tableIndex;
    private Segment currentSegment;

    private TableImpl(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) {
        this.tableName = tableName;
        this.tablePath = pathToDatabaseRoot.resolve(tableName);

        this.tableIndex = tableIndex;
    }

    private TableImpl(TableInitializationContext context) {
        this.tableName = context.getTableName();
        this.tablePath = context.getTablePath();
        this.tableIndex = context.getTableIndex();
        this.currentSegment = context.getCurrentSegment();
    }

    static Table create(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) throws DatabaseException {
        TableImpl tb = new TableImpl(tableName, pathToDatabaseRoot, tableIndex);
        tb.initializeAsNew();
        return new CachingTable(tb);
    }

    @Override
    public String getName() {
        //todo
        return null;
    }

    @Override
    public void write(String objectKey, String objectValue) throws DatabaseException {
        //todo
    }

    @Override
    public String read(String objectKey) throws DatabaseException {
        //todo
        return null;
    }

    private void initializeAsNew() throws DatabaseException {
        //todo
    }
}