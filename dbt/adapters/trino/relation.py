from dataclasses import dataclass, field

from dbt.adapters.base.relation import BaseRelation, Policy
from dbt.adapters.contracts.relation import ComponentName


@dataclass(frozen=True, eq=False, repr=False)
class TrinoRelation(BaseRelation):
    quote_policy: Policy = field(default_factory=lambda: Policy())

    # Overridden as Trino converts relation identifiers to lowercase
    def _is_exactish_match(self, field: ComponentName, value: str) -> bool:
        return self.path.get_lowered_part(field) == value.lower()
