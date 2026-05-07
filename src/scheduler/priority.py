from src.models.workflow import JobPriority


class PriorityPressurePolicy:
    """Small policy object for pressure behavior around priority queues."""

    def should_defer_under_pressure(self, priority: JobPriority, utilization: float) -> bool:
        if priority == JobPriority.LOW and utilization >= 0.75:
            return True
        if priority == JobPriority.MEDIUM and utilization >= 0.95:
            return True
        return False


priority_pressure_policy = PriorityPressurePolicy()
