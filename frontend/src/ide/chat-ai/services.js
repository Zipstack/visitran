import Cookies from "js-cookie";

import { useAxiosPrivate } from "../../service/axios-service";
import { orgStore } from "../../store/org-store";
import { useProjectStore } from "../../store/project-store";

export function useChatAIService() {
  const axiosPrivate = useAxiosPrivate();
  const csrfToken = Cookies.get("csrftoken");
  const { selectedOrgId } = orgStore();
  const { projectId } = useProjectStore();
  const orgId = selectedOrgId || "default_org";

  const getAllChats = async () => {
    const url = `/api/v1/visitran/${orgId}/project/${projectId}/chat`;
    const response = await axiosPrivate.get(url);
    return response.data || [];
  };

  const deleteChatById = async (conversationId) => {
    const url = `/api/v1/visitran/${orgId}/project/${projectId}/chat/delete/${conversationId}`;
    await axiosPrivate.delete(url, {
      headers: {
        "Content-Type": "application/json",
        "X-CSRFToken": csrfToken,
      },
    });
    return true;
  };

  const updateChatName = async (chatId, newName) => {
    const url = `/api/v1/visitran/${orgId}/project/${projectId}/chat/update/${chatId}`;
    const response = await axiosPrivate.patch(
      url,
      { chat_name: newName },
      {
        headers: {
          "Content-Type": "application/json",
          "X-CSRFToken": csrfToken,
        },
      }
    );
    return response.data;
  };

  const postChatPrompt = async ({
    prompt,
    llm_model_architect,
    llm_model_developer,
    chatId = null,
    chatIntentId = null,
    discussionStatus = null,
    chatMessageId = null,
  }) => {
    const url = `/api/v1/visitran/${orgId}/project/${projectId}/chat`;
    const response = await axiosPrivate.post(
      url,
      {
        prompt,
        llm_model_architect,
        llm_model_developer,
        chat_id: chatId,
        chat_intent_id: chatIntentId,
        discussion_status: discussionStatus,
        final_discussion_id: chatMessageId,
      },
      {
        headers: {
          "Content-Type": "application/json",
          "X-CSRFToken": csrfToken,
        },
      }
    );
    return response.data || {};
  };

  const getChatMessagesByChatId = async (chatId) => {
    const url = `/api/v1/visitran/${orgId}/project/${projectId}/chat/${chatId}/chat-message`;
    const response = await axiosPrivate.get(url);
    return response.data || [];
  };

  const getChatMessageResponseByIds = async ({ chatId, chatMessageId }) => {
    const url = `/api/v1/visitran/${orgId}/project/${projectId}/chat/${chatId}/${chatMessageId}/response`;
    const response = await axiosPrivate.get(url, {
      headers: {
        "Content-Type": "application/json",
        "X-CSRFToken": csrfToken,
      },
    });
    return response.data;
  };

  const getChatIntents = async () => {
    const url = `/api/v1/visitran/${orgId}/project/${projectId}/chat-intent`;
    const response = await axiosPrivate.get(url, {
      headers: {
        "Content-Type": "application/json",
        "X-CSRFToken": csrfToken,
      },
    });
    return response.data;
  };

  const getChatLlmModels = async () => {
    const url = `/api/v1/visitran/${orgId}/project/${projectId}/chat/list-llm-models`;
    const response = await axiosPrivate.get(url);
    return response.data;
  };

  const completeOnboardingTask = async (taskId) => {
    const url = `/api/v1/visitran/${orgId}/project/${projectId}/onboarding/complete-task/`;
    const response = await axiosPrivate.post(
      url,
      { task_id: taskId },
      {
        headers: {
          "Content-Type": "application/json",
          "X-CSRFToken": csrfToken,
        },
      }
    );
    return response.data;
  };

  const getOnboardingStatus = async () => {
    const url = `/api/v1/visitran/${orgId}/project/${projectId}/onboarding/status/`;
    const response = await axiosPrivate.get(url, {
      headers: {
        "Content-Type": "application/json",
        "X-CSRFToken": csrfToken,
      },
    });
    return response.data;
  };

  return {
    getAllChats,
    deleteChatById,
    updateChatName,
    postChatPrompt,
    getChatMessagesByChatId,
    getChatMessageResponseByIds,
    getChatIntents,
    getChatLlmModels,
    completeOnboardingTask,
    getOnboardingStatus,
  };
}
