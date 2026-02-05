# Contributing

PRs welcome. Here's what helps.

## Before You Start

- Run `mvn test` and make sure it passes
- Read the code you're changing, the style is intentionally boring

## What I'll Merge

- Bug fixes with tests
- Performance improvements with benchmarks
- Documentation fixes
- Test coverage improvements

## What I Probably Won't

- New dependencies (the zero-deps thing is on purpose)
- Major architectural changes without discussion first
- Features that complicate the core Raft logic

## Code Style

- Standard Java conventions
- No comments unless the code is genuinely confusing
- Keep methods short
- Tests go in the matching package under `src/test`

## Running Tests

```bash
mvn test
```

If tests are flaky on your machine, open an issue.

## Questions

Open an issue. I'll get to it when I can.
