# faststream-sqlbroker

A SQL-backed broker for [FastStream](https://github.com/ag2ai/FastStream).

## Origings

Originated as a [PR to FastStream](https://github.com/ag2ai/faststream/pull/2704).

## Development

The test suite reuses upstream FastStream's testing primitives (e.g.
`BrokerRealConsumeTestcase`), which live in the FastStream repo's `tests/`
tree rather than the published package. As a temporary measure until a
package shipping those tests is published, we pull them in via a git
submodule at `./faststream/`; `tests/__init__.py` extends `tests.__path__`
so `from tests.brokers.base.consume import ...` resolves through the
submodule.

```bash
git clone --recurse-submodules <repo-url>
# or, after a plain clone:
git submodule update --init
```

The runtime `__path__` trick is invisible to static analyzers, so
basedpyright flags `from tests.brokers... import ...` as unresolved. If
that bothers you, symlink the upstream subtree into `tests/`:

```bash
ln -s ../faststream/tests/brokers tests/brokers  # opt-in
rm tests/brokers                                 # to undo
```

To point the submodule at a local FastStream working copy (e.g. when
co-developing both repos):

```bash
git config submodule.faststream.url ../path/to/faststream
git submodule sync
```

CI fetches the submodule automatically via `submodules: recursive` on
`actions/checkout`.

### Docs

Docs live under `docs/` mirroring upstream FastStream's layout
(`docs/mkdocs.yml`, pages at `docs/docs/sqla/*.md`, code samples at
`docs/docs_src/sqla/*.py`). The upstream FastStream site pulls them in at
build time via
[`mkdocs-multirepo-plugin`](https://github.com/jdoiro3/mkdocs-multirepo-plugin):
its `navigation_template.txt` carries an `!import` pointing at this repo's
`main` branch, and edit links / repo URLs on the imported pages resolve to
`faststream-community/faststream-sqlbroker` via this repo's own
`repo_url` / `edit_uri`.
