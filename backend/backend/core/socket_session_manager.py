class SocketSessionContext:
    _instance = None
    _sid_to_context = {}

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(SocketSessionContext, cls).__new__(cls)
        return cls._instance

    def set_context(self, sid, user, tenant, env):
        self._sid_to_context[sid] = {
            "user": user,
            "tenant": tenant,
            "env": env
        }

    def get_context(self, sid):
        return self._sid_to_context.get(sid, {})

    def clear_context(self, sid):
        if sid in self._sid_to_context:
            del self._sid_to_context[sid]

    def get_active_sessions(self):
        """Get list of all active session IDs."""
        return list(self._sid_to_context.keys())

    def get_session_count(self):
        """Get the count of active sessions."""
        return len(self._sid_to_context)
