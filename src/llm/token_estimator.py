from src.config import settings


class TokenEstimator:
    def estimate(self, prompt_or_transcript: str, completion_buffer: int = 500) -> int:
        # A conservative approximation for English/Hinglish text. The scheduler
        # reserves with a safety factor so provider-specific tokenization drift
        # does not turn into a 429.
        estimated_prompt_tokens = max(len(prompt_or_transcript) // 4, 1)
        raw_estimate = estimated_prompt_tokens + completion_buffer
        return min(
            int(raw_estimate * settings.LLM_RESERVATION_SAFETY_FACTOR),
            settings.LLM_MAX_PROMPT_TOKENS,
        )


token_estimator = TokenEstimator()
