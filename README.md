# faststream-sqlbroker

A SQL-backed broker for [FastStream](https://github.com/ag2ai/FastStream). Originated
as a [PR to FastStream](https://github.com/ag2ai/faststream/pull/2704).

## Development

The test suite reuses upstream FastStream's `Testcase` mixins (e.g.
`BrokerRealConsumeTestcase`), which live in the FastStream repo's `tests/`
tree rather than the published package. We pull them in via a git submodule
at `./faststream/`, and `tests/__init__.py` extends `tests.__path__` so
`from tests.brokers.base.consume import ...` resolves against the submodule.

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
