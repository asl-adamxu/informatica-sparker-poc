"""Load configuration from the converter's ``env/config.yml``.

The converter already generates ``env/config.yml`` with all database
connection definitions.  The Validation Framework reuses this as its
single source of truth for connection information.

The top-level ``connections:`` section is used by both the converter
and the Validation Framework (for target queries).

Validation-specific overrides live under ``validation:`` so they can
never affect converter behaviour:

    # env/config.yml
    connections:                     # ← used by converter + validation
      oracle-defaults: &oracle
        host: host1
        username: pyspark
        schema: pyspark
        ...

      DPA:
        <<: *oracle
        database: DPA

    validation:                      # ← ONLY read by validation framework
      connections:                   #   fields to override for source queries
        host: host2
        username: informatica
        schema: informatica

When the validation framework resolves a connection with an environment
(e.g. ``environment="informatica"``):

1. Load the base connection from ``connections.<name>``.
2. Merge ``validation.connections`` overrides on top.
3. Return the merged result.

When no environment is specified, the base connection is returned
unchanged — this is the PySpark (target) side.
"""

from typing import Optional

try:
    import yaml
except ImportError:
    yaml = None  # type: ignore


def load_config(config_path: str) -> dict:
    """Load a YAML config file (typically ``env/config.yml``)."""
    if yaml is None:
        raise ImportError(
            "PyYAML is required to load configuration. "
            "Install with: pip install pyyaml"
        )
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _find_connection(connections: dict, name: str) -> Optional[dict]:
    """Exact or prefix match of *name* in *connections*."""
    if name in connections:
        return connections[name]
    for key, val in connections.items():
        if name.startswith(key) or key.startswith(name):
            return val
    return None


def resolve_connection(
    config: dict,
    connection_name: str,
    environment: Optional[str] = None,
) -> Optional[dict]:
    """Look up a connection by name, optionally applying validation overrides.

    Resolution:
    1. Load the base connection from ``connections.<name>``.
    2. If *environment* is set, merge ``validation.connections`` overrides.
    3. Return the result (or ``None`` if not found).

    Args:
        config: The full config dict (loaded from ``env/config.yml``).
        connection_name: Logical name from metadata.json (e.g. ``"DPA"``).
        environment: When set (e.g. ``"informatica"``), applies
                     ``validation.connections`` as overrides for source queries.

    Returns:
        Connection config dict or ``None`` if not found.
    """
    base = _find_connection(config.get("connections", {}), connection_name)
    if base is None:
        return None

    if environment:
        overrides = config.get("validation", {}).get("connections", {})
        if overrides:
            return {**base, **overrides}

    return base
