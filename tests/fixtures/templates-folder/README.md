# Engine-test template folder (frozen snapshot)

`tests/conftest.py` puts this folder first in `BRASA_TEMPLATE_PATH` for the
whole test session, so engine tests load templates from here — never from
`brasa/files/templates/` (removed in 0.5.0) nor from brasa-templates.

It is a **frozen snapshot** of the templates taken on 2026-10-31 (WIL-197).
Do not update it to follow brasa-templates: change it only when an engine
change needs it (for example a template-format migration). It can be replaced
by small hand-written fixtures, test file by test file.
