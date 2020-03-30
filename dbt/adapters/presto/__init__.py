from dbt.adapters.presto.connections import PrestoConnectionManager  # noqa
from dbt.adapters.presto.connections import PrestoCredentials
from dbt.adapters.presto.column import PrestoColumn  # noqa
from dbt.adapters.presto.impl import PrestoAdapter

from dbt.adapters.base import AdapterPlugin
from dbt.include import presto


Plugin = AdapterPlugin(
    adapter=PrestoAdapter,
    credentials=PrestoCredentials,
    include_path=presto.PACKAGE_PATH)
