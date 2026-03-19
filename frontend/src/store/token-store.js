import { create } from "zustand";

/**
 * Global token balance store for organization-level token tracking
 * Maintains token balance across all chats in the session
 * Updates via WebSocket and initial API fetch
 */
const useTokenStore = create((set) => ({
  // Token balance data
  tokenBalance: null,
  lastUpdated: null,
  isLoading: false,
  error: null,

  /**
   * Set token balance from API or WebSocket
   * @param {Object} tokenData - Token data object
   * @param {number} tokenData.remaining_balance - Current balance
   * @param {number} tokenData.total_consumed - Total tokens consumed
   * @param {number} tokenData.total_purchased - Total tokens purchased
   * @param {number} tokenData.utilization_percentage - Usage percentage
   * @return {void}
   */
  setTokenBalance: (tokenData) =>
    set({
      tokenBalance: {
        current_balance:
          tokenData.remaining_balance || tokenData.current_balance,
        total_consumed: tokenData.total_consumed,
        total_purchased: tokenData.total_purchased,
        utilization_percentage: tokenData.utilization_percentage,
      },
      lastUpdated: new Date().toISOString(),
      error: null,
    }),

  /**
   * Update token balance from WebSocket message
   * @param {Object} socketData - Socket message data
   */
  updateFromWebSocket: (socketData) => {
    if (socketData.token_usage_data) {
      const data = socketData.token_usage_data;
      set({
        tokenBalance: {
          current_balance: data.remaining_balance || data.current_balance,
          total_consumed: data.total_consumed,
          total_purchased: data.total_purchased,
          utilization_percentage: data.utilization_percentage,
        },
        lastUpdated: new Date().toISOString(),
        error: null,
      });
    }
  },

  /**
   * Set loading state
   * @param {boolean} isLoading - Loading state
   * @return {void}
   */
  setLoading: (isLoading) => set({ isLoading }),

  /**
   * Set error state
   * @param {string|null} error - Error message
   * @return {void}
   */
  setError: (error) => set({ error, isLoading: false }),

  /**
   * Clear token balance (on logout or organization change)
   * @return {void}
   */
  clearTokenBalance: () =>
    set({
      tokenBalance: null,
      lastUpdated: null,
      isLoading: false,
      error: null,
    }),
}));

export { useTokenStore };
