package ru.ifmo.database.server.logic.impl;

import netscape.javascript.JSObject;
import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.index.impl.TableIndex;
import ru.ifmo.database.server.initialization.TableInitializationContext;
import ru.ifmo.database.server.logic.Segment;
import ru.ifmo.database.server.logic.Table;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;

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

    private String tableName;
    private Path fileName;

    void create(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) throws DatabaseException {
        try {
            this.tableName = tableName;
            Path path = pathToDatabaseRoot.toAbsolutePath();
            path.endsWith(Path.of(tableName));
            Files.createDirectories(path);
            //String fileName = tableName + "_0";
            path.endsWith(Path.of(tableName + "_0"));
            fileName = Files.createFile(path);
        } catch (IOException e) {
            throw new DatabaseException("IOException", e);
        }
       // return Table;
    }

    public static Table initializeFromContext(TableInitializationContext context) {
        throw new UnsupportedOperationException(); // todo implement
    }

    @Override
    public String getName() {
        return tableName;
    }

    @Override
    public void write(String objectKey, String objectValue) throws DatabaseException {
        BufferedWriter writer;
        try {
            String str = objectKey + ":" + objectValue;
            writer = new BufferedWriter(new FileWriter(String.valueOf(fileName), true));
            writer.append(' ');
            writer.append(str);
            writer.close();

        } catch (IOException e) {
            throw new DatabaseException("Can't write to file", e);

        }

    }

    @Override
    public String read(String objectKey) throws DatabaseException {
        throw new UnsupportedOperationException(); // todo implement
    }
}