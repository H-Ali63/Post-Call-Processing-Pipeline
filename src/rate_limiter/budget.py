from __future__ import annotations

from dataclasses import dataclass

from src.models.workflow import JobPriority


@dataclass(frozen=True)
class BudgetState:
    token_limit: int
    request_limit: int
    reserved_tokens: int = 0
    reserved_requests: int = 0


@dataclass(frozen=True)
class BudgetDecision:
    allowed: bool
    reason: str


class BudgetAdmissionPolicy:
    def decide(
        self,
        *,
        global_state: BudgetState,
        customer_state: BudgetState,
        estimated_tokens: int,
        priority: JobPriority,
    ) -> BudgetDecision:
        if global_state.reserved_tokens + estimated_tokens > global_state.token_limit:
            return BudgetDecision(False, "GLOBAL_RATE_LIMIT")

        if global_state.reserved_requests + 1 > global_state.request_limit:
            return BudgetDecision(False, "GLOBAL_RATE_LIMIT")

        if customer_state.reserved_tokens + estimated_tokens <= customer_state.token_limit:
            return BudgetDecision(True, "ADMITTED")

        global_utilization = (
            global_state.reserved_tokens / global_state.token_limit
            if global_state.token_limit
            else 1
        )
        if priority == JobPriority.HIGH and global_utilization < 0.70:
            return BudgetDecision(True, "ADMITTED_OVERFLOW")

        return BudgetDecision(False, "CUSTOMER_TOKEN_BUDGET")


budget_admission_policy = BudgetAdmissionPolicy()
