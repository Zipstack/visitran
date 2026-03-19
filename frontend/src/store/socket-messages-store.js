import { create } from "zustand";
import { v4 as uuidv4 } from "uuid";
import yaml from "js-yaml";

/**
 * Zustand store for managing and batching socket messages.
 */
const STORE_VARIABLES = {
  socketMessages: [],
  selectedChatResponse: null,
  generateModelMap: {}, // { [chat_message_id]: { status: string, models: string[] } }
};

const useSocketMessagesStore = create((set, get) => {
  // Adds one or more messages to the store
  const addMessages = (incomingMessages) => {
    set((state) => {
      const updatedSocketMessages = [...state.socketMessages];
      const updatedGenerateModelMap = { ...state.generateModelMap };

      incomingMessages.forEach((msg) => {
        const newMsg = { ...msg, uuid: msg.uuid || uuidv4() };
        updatedSocketMessages.push(newMsg);

        const chatMessageId = newMsg.chat_message_id;
        // Ensure valid structure before updating the map
        const modelsFromSocket =
          newMsg.generated_models ?? newMsg.generate_model_list;

        if (
          chatMessageId &&
          typeof newMsg.generate_model_status === "string" &&
          Array.isArray(modelsFromSocket)
        ) {
          updatedGenerateModelMap[chatMessageId] = {
            status: newMsg.generate_model_status,
            models: modelsFromSocket,
          };
        }
      });

      return {
        socketMessages: updatedSocketMessages,
        generateModelMap: updatedGenerateModelMap,
      };
    });
  };

  // Removes one or more messages by uuid
  const removeMessages = (uuids) => {
    set((state) => ({
      socketMessages: state.socketMessages.filter(
        (msg) => !uuids.includes(msg?.uuid)
      ),
    }));
  };

  return {
    ...STORE_VARIABLES,

    pushMessage: (message) => {
      addMessages([message]);
    },

    pushMessagesBatch: (messages) => {
      addMessages(messages);
    },

    removeMessage: (uuid) => {
      removeMessages([uuid]);
    },

    removeMessagesBatch: (uuids) => {
      removeMessages(uuids);
    },

    setTransformApply: (yamlResponse) => {
      try {
        const jsonResponse = yaml.load(yamlResponse);
        const responseArray = Array.isArray(jsonResponse)
          ? jsonResponse
          : [jsonResponse];
        set({ selectedChatResponse: responseArray });
      } catch (error) {
        console.error("Error processing YAML response:", error);
        console.error("Raw YAML that caused error:", yamlResponse);
      }
    },
  };
});

export { useSocketMessagesStore };
