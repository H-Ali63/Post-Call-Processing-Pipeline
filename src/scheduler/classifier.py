from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

from src.models.workflow import JobPriority


@dataclass(frozen=True)
class Classification:
    skip_llm: bool
    priority: JobPriority
    call_stage: str
    reason: str


class InteractionClassifier:
    high_keywords = {
        "confirmed",
        "booked",
        "demo",
        "appointment",
        "reschedule",
        "manager",
        "complaint",
        "escalate",
        "unacceptable",
    }
    medium_keywords = {"callback", "call back", "later", "baad mein", "follow-up"}
    low_keywords = {
        "not interested",
        "dont call",
        "don't call",
        "wrong number",
        "already booked",
        "already purchased",
        "already done",
    }

    def classify(
        self,
        *,
        transcript: List[Dict[str, Any]],
        transcript_text: str,
        additional_data: Dict[str, Any],
    ) -> Classification:
        if len(transcript) < 4:
            return Classification(
                skip_llm=True,
                priority=JobPriority.LOW,
                call_stage="short_call",
                reason="short_transcript",
            )

        disposition = str(additional_data.get("disposition") or "").lower()
        text = f"{transcript_text} {disposition}".lower()

        if self._contains_any(text, self.high_keywords):
            return Classification(
                skip_llm=False,
                priority=JobPriority.HIGH,
                call_stage="needs_analysis",
                reason="high_value_keyword",
            )

        if self._contains_any(text, self.medium_keywords):
            return Classification(
                skip_llm=False,
                priority=JobPriority.MEDIUM,
                call_stage="needs_analysis",
                reason="medium_value_keyword",
            )

        if self._contains_any(text, self.low_keywords):
            return Classification(
                skip_llm=False,
                priority=JobPriority.LOW,
                call_stage="needs_analysis",
                reason="low_value_keyword",
            )

        return Classification(
            skip_llm=False,
            priority=JobPriority.MEDIUM,
            call_stage="needs_analysis",
            reason="default_medium",
        )

    def _contains_any(self, text: str, keywords: set[str]) -> bool:
        return any(keyword in text for keyword in keywords)


interaction_classifier = InteractionClassifier()
