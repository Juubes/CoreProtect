package net.coreprotect.listener.entity;

import org.bukkit.Material;
import org.bukkit.block.Block;
import org.bukkit.block.BlockFace;
import org.bukkit.entity.Entity;
import org.bukkit.entity.Hanging;
import org.bukkit.entity.ItemFrame;
import org.bukkit.entity.Painting;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.event.hanging.HangingBreakEvent;

import net.coreprotect.bukkit.BukkitAdapter;
import net.coreprotect.config.Config;
import net.coreprotect.consumer.Queue;
import net.coreprotect.utility.Util;

public final class HangingBreakListener extends Queue implements Listener {

    @EventHandler(priority = EventPriority.MONITOR)
    protected void onHangingBreak(HangingBreakEvent event) {
        HangingBreakEvent.RemoveCause cause = event.getCause();
        Entity entity = event.getEntity();
        Block blockEvent = event.getEntity().getLocation().getBlock();

        if (entity instanceof ItemFrame || entity instanceof Painting) {
            if (cause.equals(HangingBreakEvent.RemoveCause.EXPLOSION)
                    || cause.equals(HangingBreakEvent.RemoveCause.PHYSICS)
                    || cause.equals(HangingBreakEvent.RemoveCause.OBSTRUCTION)) {
                String causeName = "#explosion";
                Block attachedBlock = null;

                if (cause.equals(HangingBreakEvent.RemoveCause.PHYSICS)) {
                    causeName = "#physics";
                } else if (cause.equals(HangingBreakEvent.RemoveCause.OBSTRUCTION)) {
                    causeName = "#obstruction";
                }

                if (!cause.equals(HangingBreakEvent.RemoveCause.EXPLOSION)) {
                    Hanging hangingEntity = (Hanging) entity;
                    BlockFace attached = hangingEntity.getAttachedFace();
                    attachedBlock = hangingEntity.getLocation().getBlock().getRelative(attached);
                }

                Material material;
                int itemData = 0;
                if (entity instanceof ItemFrame) {
                    material = BukkitAdapter.ADAPTER.getFrameType(entity);
                    ItemFrame itemframe = (ItemFrame) entity;

                    if (itemframe.getItem() != null) {
                        itemData = Util.getBlockId(itemframe.getItem().getType());
                    }
                } else {
                    material = Material.PAINTING;
                    Painting painting = (Painting) entity;
                    itemData = Util.getArtId(painting.getArt().toString(), true);
                }

                if (!event.isCancelled() && Config.getConfig(blockEvent.getWorld()).NATURAL_BREAK) {
                    Queue.queueNaturalBlockBreak(causeName, blockEvent.getState(), attachedBlock, material, itemData);
                }
            }
        }
    }
}
