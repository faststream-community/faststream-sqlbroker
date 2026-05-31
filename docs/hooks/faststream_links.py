"""Build hooks for the standalone SqlBroker docs site.

1. ``on_config`` makes the build directory-independent. The ``mdx_include``
   extension resolves ``{!> docs_src/... !}`` includes against ``base_path``,
   which is relative to the *current working directory* — not the config file.
   So ``mkdocs ... -f docs/mkdocs.yml`` run from the repo root would leave every
   code sample empty. We rewrite ``base_path`` to the absolute config directory
   so the documented commands work from anywhere (a ``chdir`` would break
   ``mkdocs serve``, which re-resolves the ``-f`` path against the CWD).

2. ``on_page_markdown`` rewrites cross-references into the main FastStream docs
   to absolute URLs. The markdown under ``docs/docs/sqlbroker/`` is shared with
   the FastStream integration build, where links such as
   ``../getting-started/acknowledgement.md`` resolve against pages that live in
   the FastStream repo. In this standalone site those pages do not exist, so we
   point them at the published FastStream docs instead. Keeping the rewrite in a
   hook lets the source markdown stay relative (correct for the integration
   build) while the standalone build still passes ``mkdocs build --strict``.
"""

import re
from pathlib import Path

FASTSTREAM_DOCS_BASE = "https://faststream.ag2.ai/latest"


def on_config(config: object, **kwargs: object) -> object:
    # Resolve docs_src includes regardless of where mkdocs was invoked from by
    # pinning mdx_include's base_path to the (absolute) config directory.
    config_file = getattr(config, "config_file_path", None)
    mdx_configs = getattr(config, "mdx_configs", None)
    if config_file and isinstance(mdx_configs, dict):
        base_path = str(Path(config_file).parent)
        mdx_configs.setdefault("mdx_include", {})["base_path"] = base_path
    return config


# ](../getting-started/foo/index.md)  -> ](https://.../getting-started/foo/)
# ](../getting-started/foo.md)        -> ](https://.../getting-started/foo/)
_LINK_RE = re.compile(r"\]\(\.\./(getting-started/[\w/-]+?)(?:/index)?\.md\)")


def _to_absolute(match: re.Match[str]) -> str:
    path = match.group(1)
    return f"]({FASTSTREAM_DOCS_BASE}/{path}/)"


def on_page_markdown(markdown: str, **kwargs: object) -> str:
    return _LINK_RE.sub(_to_absolute, markdown)
