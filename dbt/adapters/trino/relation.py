from dataclasses import dataclass, field

from dbt.adapters.base.relation import BaseRelation, EventTimeFilter, Policy
from dbt.adapters.contracts.relation import ComponentName


@dataclass(frozen=True, eq=False, repr=False)
class TrinoRelation(BaseRelation):
    quote_policy: Policy = field(default_factory=lambda: Policy())
    require_alias: bool = False

    # Overridden as Trino converts relation identifiers to lowercase
    def _is_exactish_match(self, field: ComponentName, value: str) -> bool:
        return self.path.get_lowered_part(field) == value.lower()

    # Overridden because Trino cannot compare a TIMESTAMP column with a VARCHAR literal.
    def _render_event_time_filtered(self, event_time_filter: EventTimeFilter) -> str:
        """
        Returns "" if start and end are both None
        """
        filter = ""
        if event_time_filter.start and event_time_filter.end:
            filter = f"{event_time_filter.field_name} >= TIMESTAMP '{event_time_filter.start}' and {event_time_filter.field_name} < TIMESTAMP '{event_time_filter.end}'"
        elif event_time_filter.start:
            filter = f"{event_time_filter.field_name} >= TIMESTAMP '{event_time_filter.start}'"
        elif event_time_filter.end:
            filter = f"{event_time_filter.field_name} < TIMESTAMP '{event_time_filter.end}'"

        return filter
