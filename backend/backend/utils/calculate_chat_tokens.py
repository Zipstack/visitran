"""Token cost calculation dispatcher.

On OSS, returns a default value of 1 (no billing). On Cloud,
pluggable_apps.subscriptions.billing provides the real implementation
that maps LLM models and chat intents to credit costs.
"""

try:
    from pluggable_apps.subscriptions.billing import calculate_chat_tokens
except ImportError:
    def calculate_chat_tokens(*args, **kwargs) -> int:
        # OSS mode: no billing, return a neutral default
        return 1
