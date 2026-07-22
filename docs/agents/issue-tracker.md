# Issue tracker: GitHub

Issues and PRDs for this repo live as GitHub issues. Use the `gh` CLI for all operations.

## Conventions

- Create an issue with `gh issue create`.
- Read an issue with `gh issue view <number> --comments`.
- List issues with `gh issue list` and appropriate label and state filters.
- Comment with `gh issue comment <number>`.
- Apply or remove labels with `gh issue edit`.
- Close issues with `gh issue close`.
- Infer the repository from the local Git remote.

## Pull requests as a triage surface

**PRs as a request surface: no.**

## Skill conventions

- When a skill says "publish to the issue tracker", create a GitHub issue.
- When a skill says "fetch the relevant ticket", use `gh issue view <number> --comments`.
- `to-spec` applies the `spec` label to issues containing completed specifications.
- Use GitHub native sub-issues and issue dependencies when available.
- Use `ready-for-agent` for issues ready for autonomous implementation.
