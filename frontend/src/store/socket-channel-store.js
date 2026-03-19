import { create } from "zustand";

export const useSocketChannelStore = create((set) => ({
  activeChannels: new Set(),
  activeChannelMeta: new Map(),

  addChannel: (channelId, meta) =>
    set((state) => {
      const updatedChannels = new Set(state?.activeChannels ?? []);
      const updatedMeta = new Map(state?.activeChannelMeta ?? []);
      updatedChannels.add(channelId);
      updatedMeta.set(channelId, meta);
      return {
        activeChannels: updatedChannels,
        activeChannelMeta: updatedMeta,
      };
    }),

  clear: () => ({ activeChannels: new Set(), activeChannelMeta: new Map() }),
}));
