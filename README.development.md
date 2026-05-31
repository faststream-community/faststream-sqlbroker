
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
