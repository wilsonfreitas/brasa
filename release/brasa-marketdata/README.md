# brasa-marketdata is now brasa-core

This package was renamed to [**brasa-core**](https://pypi.org/project/brasa-core/)
in version 0.5.0. To switch:

    pip uninstall brasa-marketdata
    pip install brasa-core

`import brasa` and the `brasa` command are unchanged.

This last `brasa-marketdata` release (0.5.0) installs nothing: installing or
upgrading to it stops with the message above, and an installed 0.4.0 keeps
working.

Since 0.5.0 the templates live in their own repo,
[brasa-templates](https://github.com/wilsonfreitas/brasa-templates). After
switching, run `brasa init` to get them. See the
[changelog](https://github.com/wilsonfreitas/brasa/blob/main/CHANGELOG.md).
