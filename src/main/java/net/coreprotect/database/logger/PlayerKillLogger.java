package net.coreprotect.database.logger;

import java.sql.PreparedStatement;
import java.util.Locale;

import org.bukkit.block.BlockState;

import net.coreprotect.CoreProtect;
import net.coreprotect.config.ConfigHandler;
import net.coreprotect.database.statement.BlockStatement;
import net.coreprotect.database.statement.UserStatement;
import net.coreprotect.event.CoreProtectPreLogEvent;
import net.coreprotect.utility.Util;

public class PlayerKillLogger {

    private PlayerKillLogger() {
        throw new IllegalStateException("Database class");
    }

    public static void log(PreparedStatement preparedStmt, int batchCount, String user, BlockState block,
            String player) {
        try {
            if (ConfigHandler.blacklist.get(user.toLowerCase(Locale.ROOT)) != null) {
                return;
            }

            CoreProtectPreLogEvent event = new CoreProtectPreLogEvent(user);
            CoreProtect.getInstance().getServer().getPluginManager().callEvent(event);

            int userId = UserStatement.getId(preparedStmt, event.getUser(), true);
            int playerId = ConfigHandler.playerIdCache.get(player.toLowerCase(Locale.ROOT));
            int wid = Util.getWorldId(block.getWorld().getName());
            int time = (int) (System.currentTimeMillis() / 1000L);
            int x = block.getX();
            int y = block.getY();
            int z = block.getZ();
            BlockStatement.insert(preparedStmt, batchCount, time, userId, wid, x, y, z, 0, playerId, null, null, 3, 0);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
