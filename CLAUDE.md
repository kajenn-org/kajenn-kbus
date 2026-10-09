# Claude Code Instructions - kajenn-kbus

**Parent Document**: [kajenn-meta CLAUDE.md](https://github.com/kajenn-org/kajenn-meta/blob/main/CLAUDE.md)
— read it first. Local checkout: `../kajenn-meta/CLAUDE.md`.

## Project-Specific Context

### Current Status
- Development Status: Pre-Alpha — Has Implementation: Yes
- Package `kbus`, repo `kajenn-kbus`. Started 2026-10-09 from the design in
  `README.md` and `docs/design/scenarios.md`.

### Content
Three layers: `kbus.core` (connections, messages, streams over three
transports), `kbus.dispatcher` (named members, routes), `kbus.link` (between
applications, token trust). `README.md` is the specification; `tests/` is the
contract, written against the public API only.

### Independence
`kbus` imports nothing from `kajenn`, `kajenn_orchestra` or any `genro-*`
package. The only runtime dependency is `websockets`. kajenn, orchestra and
Sourcerer depend on kbus, never the reverse.

### Git hooks
```bash
cp hooks/pre-commit hooks/pre-push hooks/commit-msg .git/hooks/ && chmod +x .git/hooks/pre-commit .git/hooks/pre-push .git/hooks/commit-msg
```

---
**All general policies are inherited from the parent document.**
