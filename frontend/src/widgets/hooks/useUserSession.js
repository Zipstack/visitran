import { useAxiosPrivate } from "../../service/axios-service";
import { useSessionStore } from "../../store/session-store";
const useUserSession = () => {
  const axios = useAxiosPrivate();
  const { setSessionDetails } = useSessionStore();

  return async () => {
    try {
      const requestOptions = {
        method: "GET",
        url: `${window.location.origin}/api/v1/session`,
      };
      const res = await axios(requestOptions);
      if (res.data) {
        setSessionDetails(res.data);
      }
      return { data: res.data, status: res.status };
    } catch (error) {
      if (error?.response?.data?.type === "subscription_error") {
        return;
      }
    }
  };
};
export { useUserSession };
