# Python Guidelines

Also see [root AGENTS.md](../AGENTS.md) for cross-language standards.

## Commands

* Environment: use `uv` for all local Python environment setup in this repository.
* First step in every new worktree or fresh checkout: run `uv sync` from `python/` before any Python command. Dev and test dependencies are included by default via `[tool.uv] default-groups`. Add `--group benchmarks`, `--group torch`, or `--group geo` only when needed.
* `uv sync` builds the local `pylance` Rust extension as part of environment setup. This can take a long time. Start it early, let it finish, and do not interrupt it or switch to a different setup path just because the build is slow.
* After the initial `uv sync`, use `uv run --frozen make test` to run tests. The `--frozen` flag skips dependency resolution and prevents lock file changes — it is much faster on subsequent runs.
* Only run `uv sync` again when dependencies change (e.g., after pulling new commits that update `pyproject.toml` or `uv.lock`).
* Command execution: always use `uv run ...` for Python-related repository commands. Do not rely on a globally activated environment.
* Never invoke bare `python`, `pytest`, `pip`, `maturin`, `make test`, `make doctest`, `make lint`, or `make format` for repository work.
* If a Python command fails outside `uv run`, that does not count as a dependency or test failure. Fix the environment usage first and rerun correctly.
* Build time expectations: `uv sync` and `uv run maturin develop` build the local `pylance` Rust extension as part of the environment workflow. This can be slow, especially on the first run or after Rust dependency changes; treat that as expected and do not switch to a different environment manager or shortcut around the build just because it takes time.
* Build: `uv run maturin develop` (required after Rust changes)
* Test: `uv run --frozen make test`
* Run single test: `uv run --frozen pytest python/tests/<test_file>.py::<test_name>`
* Doctest: `uv run --frozen make doctest`
* Lint: `uv run --frozen make lint`
* Format: `uv run --frozen make format`

## API Design

- Keep bindings as thin wrappers — centralize validation and logic in Rust core.
- Extend existing methods with named arguments instead of adding new methods that accept policy/config objects — the Python API should feel Pythonic (e.g., `cleanup_old_versions(..., retain_versions=N)`), not mirror Rust builder patterns.
- Pass all fields to Python dataclass constructors via PyO3, converting Rust `None` to `py.None()` instead of omitting args — dataclass constructors require all positional params.
- Use parameterized type hints (e.g., `list[DatasetBasePath]`, `Optional[Dict[str, str]]`) — never bare generics. Keep docstring type descriptions in sync with hints.

## Testing

- Use `@pytest.mark.parametrize` for tests that differ only in inputs — extract shared setup into helpers.
- Add tests to existing `test_{module}.py` files rather than creating new test files for the same module.
- Replace `print()` in tests with `assert` statements.

## Common Failure Mode

- A missing module or missing command error from bare `python`, `pytest`, `pip`, `maturin`, or `make` is usually an environment usage mistake, not a repository issue.
- Before reporting a Python dependency as unavailable, verify that `uv sync` has been run in the current worktree and that the failing command was executed with `uv run ...`.
