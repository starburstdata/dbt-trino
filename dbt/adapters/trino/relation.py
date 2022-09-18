from dataclasses import dataclass

from dbt.adapters.base.relation import BaseRelation, Policy


@dataclass
class TrinoQuotePolicy(Policy):
    database: bool = False
    schema: bool = False
    identifier: bool = False


@dataclass(frozen=True, eq=False, repr=False)
class TrinoRelation(BaseRelation):
    quote_policy: TrinoQuotePolicy = TrinoQuotePolicy()
