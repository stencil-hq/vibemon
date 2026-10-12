# Copyright 2023 FoundryLabs, Inc. (E2B)
# SPDX-License-Identifier: Apache-2.0
#
# Adapted from e2b-dev/desktop `template/template.py` at
# 1ff98a36306989d155ce5eceab2a2d38c9a8d6d2 (see ../SOURCES.md) and
# `template/build_prod.py` (resources). Changes by vibemon:
# - Google Chrome and VS Code are installed for the guest architecture at
#   build time: upstream's amd64 Chrome repo on amd64; Chromium from the
#   xtradeb PPA on arm64 (Google ships no arm64 Linux Chrome), exposed as
#   `/usr/bin/google-chrome-stable` so the upstream `google-chrome.desktop`
#   launcher works unchanged; the VS Code repo uses the native architecture
#   instead of `[arch=amd64]`;
# - vibemon branding: `files/wallpaper.png` is the vibemon wallpaper (source
#   in `branding/`), and the noVNC page gets the vibemon title and icons
#   (`files/novnc/`) instead of the fork's E2B ones;
# - `add-apt-repository` runs with `-y` (no stdin during builds);
# - the file context is pinned to `files/` next to this file and the
#   template is returned by `make_template()`.

from pathlib import Path

from e2b import CopyItem, Template

CPU_COUNT = 8
MEMORY_MB = 8192

# Browser repositories and packages for the guest architecture.
INSTALL_CHROME = (
    'case "$(dpkg --print-architecture)" in '
    "amd64) wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - "
    '&& echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list '
    "&& apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends google-chrome-stable ;; "
    "arm64) add-apt-repository -y ppa:xtradeb/apps "
    "&& DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends chromium "
    "&& ln -sf /usr/bin/chromium /usr/bin/google-chrome-stable ;; "
    '*) echo "unsupported architecture: $(dpkg --print-architecture)" >&2; exit 1 ;; '
    "esac"
)


# Replaces the E2B title and icons of the e2b-dev/noVNC fork with vibemon's
# (copied to app/images/icons/vibemon); fails if any E2B branding remains.
REBRAND_NOVNC = (
    "sed -i"
    " -e 's|<title>E2B VNC</title>|<title>vibemon desktop</title>|'"
    " -e '/icons\\/e2b\\//d'"
    " -e 's|<!-- Favicon -->|<!-- Favicon -->\\n"
    '    <link rel="icon" type="image/svg+xml" href="app/images/icons/vibemon/favicon.svg">\\n'
    '    <link rel="icon" type="image/png" sizes="32x32" href="app/images/icons/vibemon/favicon-32x32.png">\\n'
    '    <link rel="apple-touch-icon" sizes="180x180" href="app/images/icons/vibemon/apple-touch-icon.png">'
    "|' /opt/noVNC/vnc.html"
    " && rm -rf /opt/noVNC/app/images/icons/e2b"
    " && ! grep -qi e2b /opt/noVNC/vnc.html"
)


def make_template():
    template = (
        Template(file_context_path=Path(__file__).parent / "files")
        .from_image("ubuntu:22.04")
        .set_user("root")
        .set_workdir("/")
        .set_envs(
            {
                # Avoid system prompts
                "DEBIAN_FRONTEND": "noninteractive",
                "DEBIAN_PRIORITY": "high",
                # Pip settings
                "PIP_DEFAULT_TIMEOUT": "100",
                "PIP_DISABLE_PIP_VERSION_CHECK": "1",
                "PIP_NO_CACHE_DIR": "1",
            }
        )
        # Initial system setup and packages
        .run_cmd("yes | unminimize")
        .apt_install(
            [
                "xserver-xorg",
                "x11-xserver-utils",
                "xvfb",
                "x11-utils",
                "xauth",
                "xfce4",
                "xfce4-goodies",
                "util-linux",
                "sudo",
                "curl",
                "git",
                "wget",
                "python3-pip",
                "xdotool",
                "scrot",
                "ffmpeg",
                "x11vnc",
                "net-tools",
                "netcat",
                "x11-apps",
                "libreoffice",
                "xpdf",
                "gedit",
                "xpaint",
                "tint2",
                "galculator",
                "pcmanfm",
                "software-properties-common",
                "apt-transport-https",
                "libgtk-3-bin",
            ]
        )
        .pip_install("numpy")
        # Setup NoVNC and websockify
        .git_clone(
            "https://github.com/e2b-dev/noVNC.git", "/opt/noVNC", branch="e2b-desktop"
        )
        .make_symlink("/opt/noVNC/vnc.html", "/opt/noVNC/index.html")
        .git_clone(
            "https://github.com/novnc/websockify.git",
            "/opt/noVNC/utils/websockify",
            branch="v0.12.0",
        )
        .copy("novnc", "/opt/noVNC/app/images/icons/vibemon")
        .run_cmd(REBRAND_NOVNC)
        # Install browsers and set up repositories
        .run_cmd(
            [
                "add-apt-repository -y ppa:mozillateam/ppa",
                "wget -qO- https://packages.microsoft.com/keys/microsoft.asc | apt-key add -",
                'add-apt-repository -y "deb [arch=$(dpkg --print-architecture)] https://packages.microsoft.com/repos/vscode stable main"',
                "apt-get update",
            ],
        )
        # Install browsers and VS Code
        .apt_install(["firefox-esr", "code"])
        .run_cmd(INSTALL_CHROME)
        # Configure system settings
        .make_symlink(
            "/usr/bin/xfce4-terminal.wrapper",
            "/etc/alternatives/x-terminal-emulator",
            force=True,
        )
        .run_cmd("update-alternatives --set x-www-browser /usr/bin/firefox-esr")
        .make_dir("/home/user/.config/Code/User")
        .make_dir("/home/user/.config/xfce4/xfconf/xfce-perchannel-xml/")
        .make_dir("/home/user/.config/autostart")
        .run_cmd("update-desktop-database /usr/share/applications/")
        # Copy all configuration files
        .copy_items(
            [
                CopyItem(
                    src="google-chrome.desktop",
                    dest="/usr/share/applications/google-chrome.desktop",
                ),
                CopyItem(
                    src="settings.json",
                    dest="/home/user/.config/Code/User/settings.json",
                ),
                CopyItem(
                    src="wallpaper.png",
                    dest="/usr/share/backgrounds/xfce/wallpaper.png",
                ),
                CopyItem(
                    src="xfce4-desktop.xml",
                    dest="/home/user/.config/xfce4/xfconf/xfce-perchannel-xml/xfce4-desktop.xml",
                ),
                CopyItem(
                    src="screensaver.desktop",
                    dest="/home/user/.config/autostart/screensaver.desktop",
                ),
                CopyItem(
                    src="firefox-policies.json",
                    dest="/usr/lib/firefox-esr/distribution/policies.json",
                ),
                CopyItem(
                    src="firefox-autoconfig.js",
                    dest="/usr/lib/firefox-esr/defaults/pref/autoconfig.js",
                ),
                CopyItem(src="firefox.cfg", dest="/usr/lib/firefox-esr/firefox.cfg"),
            ]
        )
    )

    # Template with user and workdir set
    return template.set_user("user").set_workdir("/home/user")
