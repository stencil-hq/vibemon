"""`base`: the default template of `Sandbox.create()`.

Upstream `base` is the `e2bdev/base` image with nothing added (the SDK's
`Template().from_base_image()`); the platform provisions envd, the default
`user`, and the init system during the build.
"""

from pathlib import Path

from e2b import Template

CPU_COUNT = 2
MEMORY_MB = 1024


def make_template():
    return Template(file_context_path=Path(__file__).parent).from_base_image()
