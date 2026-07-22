# Use Go with isolated Pi subprocesses

The backlog runner is a standalone Go program that controls one Pi subprocess per issue through Pi's language-neutral JSONL interface. This keeps issue contexts and failures isolated, produces a single operational binary, and avoids coupling the scheduler to Pi's TypeScript SDK; any future Pi extension will remain a thin TypeScript control panel over the same standalone runner.
