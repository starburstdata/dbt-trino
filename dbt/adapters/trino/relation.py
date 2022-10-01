from dataclasses import dataclass

from dbt.adapters.base.relation import BaseRelation, Policy
from dbt.contracts.relation import ComponentName


@dataclass
class TrinoQuotePolicy(Policy):
    database: bool = False
    schema: bool = False
    identifier: bool = False


@dataclass(frozen=True, eq=False, repr=False)
class TrinoRelation(BaseRelation):
    quote_policy: TrinoQuotePolicy = TrinoQuotePolicy()

    # Overridden as Trino converts relation identifiers to lowercase
    def _is_exactish_match(self, field: ComponentName, value: str) -> bool:
        return self.path.get_lowered_part(field) == value.lower()
