package net.coreprotect.database;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;

import org.bukkit.Location;
import org.bukkit.Material;
import org.bukkit.inventory.ItemStack;

import net.coreprotect.consumer.Queue;

public abstract class Database extends Queue {

    public static final int SIGN = 0;
    public static final int BLOCK = 1;
    public static final int SKULL = 2;
    public static final int CONTAINER = 3;
    public static final int WORLD = 4;
    public static final int CHAT = 5;
    public static final int COMMAND = 6;
    public static final int SESSION = 7;
    public static final int ENTITY = 8;
    public static final int MATERIAL = 9;
    public static final int ART = 10;
    public static final int ENTITY_MAP = 11;
    public static final int BLOCKDATA = 12;
    public static final int ITEM = 13;

    public abstract void beginTransaction(Statement statement);

    public abstract void commitTransaction(Statement statement) throws Exception;

    public abstract void setMultiInt(PreparedStatement statement, int value, int count);

    public abstract void containerBreakCheck(String user, Material type, Object container, ItemStack[] contents,
            Location location);

    public abstract Connection getConnection(boolean onlyCheckTransacting);

    public abstract Connection getConnection(boolean force, int waitTime);

    public abstract Connection getConnection(boolean force, boolean startup, boolean onlyCheckTransacting,
            int waitTime);

    public abstract void performUpdate(Statement statement, long id, int action, int table);

    public abstract PreparedStatement prepareStatement(Connection connection, int type, boolean keys);

    public abstract PreparedStatement prepareStatement(Connection connection, String query, boolean keys);

    protected abstract void initializeTables(String prefix, Statement statement);

    public abstract void createDatabaseTables(String prefix, boolean purge);
}
