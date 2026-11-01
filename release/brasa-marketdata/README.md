# brasa-marketdata is now brasa-core

This package was renamed to [**brasa-core**](https://pypi.org/project/brasa-core/)
in version 0.5.0. Install it with:

    pip install brasa-core

This last `brasa-marketdata` release (0.5.0) contains no code: it only depends
on `brasa-core`, so `pip install -U brasa-marketdata` also gets you brasa-core.
`import brasa` and the `brasa` command are unchanged.

Since 0.5.0 the templates live in their own repo,
[brasa-templates](https://github.com/wilsonfreitas/brasa-templates). After
upgrading, run `brasa init` to get them. See the
[changelog](https://github.com/wilsonfreitas/brasa/blob/main/CHANGELOG.md).
