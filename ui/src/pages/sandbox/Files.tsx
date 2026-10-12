import { type DragEvent, useEffect, useRef, useState } from "react";
import {
  Button,
  Card,
  confirmAction,
  Empty,
  ErrorNote,
  Menu,
  Modal,
  promptText,
} from "../../components.tsx";
import { type EntryInfo, type EnvdTarget, files } from "../../envd.ts";
import { fmt } from "../../format.ts";
import { attempt, toast, usePoll } from "../../hooks.ts";
import { Icon } from "../../icons.tsx";
import { setQuery, useRoute } from "../../router.tsx";

/** Text preview limit; larger files show their head and offer a download. */
const PREVIEW_BYTES = 256 * 1024;
const IMAGE = /\.(png|jpe?g|gif|webp|avif|bmp|ico|svg)$/i;

function join(dir: string, name: string): string {
  return dir === "/" ? `/${name}` : `${dir}/${name}`;
}

function parent(path: string): string {
  const cut = path.lastIndexOf("/");
  return cut <= 0 ? "/" : path.slice(0, cut);
}

function isDir(entry: EntryInfo): boolean {
  return entry.type === "FILE_TYPE_DIRECTORY";
}

function saveBlob(blob: Blob, name: string): void {
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = name;
  link.click();
  window.setTimeout(() => URL.revokeObjectURL(url), 10_000);
}

interface Preview {
  path: string;
  name: string;
  text?: string;
  truncated?: boolean;
  image?: string;
  binary?: boolean;
}

/** File browser over envd's filesystem service and `/files` (the default user's view). */
export function FilesPanel({ target }: { target: EnvdTarget }) {
  const route = useRoute();
  const [home, setHome] = useState<string | null>(null);
  const dir = route.query.get("path") || home;
  const [preview, setPreview] = useState<Preview | null>(null);
  const [uploads, setUploads] = useState<{ name: string; ratio: number }[]>([]);
  const [over, setOver] = useState(false);
  const picker = useRef<HTMLInputElement>(null);
  const { sandboxID, token } = target;

  useEffect(() => {
    files
      .stat({ sandboxID, token }, "~")
      .then((entry) => setHome(entry.path))
      .catch(() => setHome("/"));
  }, [sandboxID, token]);

  const listing = usePoll(
    `files|${sandboxID}|${dir}`,
    (signal) => (dir ? files.list(target, dir, signal) : Promise.resolve([])),
    0,
  );
  const entries = [...(listing.data ?? [])].sort(
    (a, b) => Number(isDir(b)) - Number(isDir(a)) || a.name.localeCompare(b.name),
  );

  const go = (path: string) => setQuery("path", path, { push: true });

  const open = async (entry: EntryInfo) => {
    if (isDir(entry)) return go(entry.path);
    if (entry.type === "FILE_TYPE_SYMLINK") {
      const resolved = await files.stat(target, entry.path).catch(() => null);
      if (resolved && isDir(resolved)) return go(entry.path);
    }
    try {
      if (IMAGE.test(entry.name)) {
        const blob = await files.download(target, entry.path);
        setPreview({ path: entry.path, name: entry.name, image: URL.createObjectURL(blob) });
        return;
      }
      const { bytes, truncated } = await files.head(target, entry.path, PREVIEW_BYTES);
      if (bytes.subarray(0, 8192).includes(0)) {
        setPreview({ path: entry.path, name: entry.name, binary: true });
      } else {
        setPreview({
          path: entry.path,
          name: entry.name,
          text: new TextDecoder().decode(bytes),
          truncated,
        });
      }
    } catch (error) {
      toast(error instanceof Error ? error.message : String(error), "error");
    }
  };

  const closePreview = () => {
    if (preview?.image) URL.revokeObjectURL(preview.image);
    setPreview(null);
  };

  const download = (path: string, name: string) =>
    void attempt(`Downloaded ${name}`, async () =>
      saveBlob(await files.download(target, path), name),
    );

  const upload = async (list: FileList | File[]) => {
    if (!dir) return;
    const chosen = Array.from(list);
    setUploads(chosen.map((file) => ({ name: file.name, ratio: 0 })));
    for (const [index, file] of chosen.entries()) {
      await attempt(`Uploaded ${file.name} (${fmt.bytes(file.size)})`, () =>
        files.upload(target, join(dir, file.name), file, (ratio) =>
          setUploads((prev) => prev.map((u, i) => (i === index ? { ...u, ratio } : u))),
        ),
      );
    }
    setUploads([]);
    listing.refresh();
  };

  const makeDir = async () => {
    if (!dir) return;
    const name = await promptText({
      title: "New folder",
      label: `Name in ${dir}`,
      action: "Create Folder",
    });
    if (!name) return;
    if (await attempt(`Created ${name}`, () => files.makeDir(target, join(dir, name))))
      listing.refresh();
  };

  const rename = async (entry: EntryInfo) => {
    const name = await promptText({
      title: `Rename ${entry.name}`,
      label: "New name",
      action: "Rename",
      initial: entry.name,
    });
    if (!name || name === entry.name) return;
    const destination = join(parent(entry.path), name);
    if (await attempt(`Renamed to ${name}`, () => files.move(target, entry.path, destination)))
      listing.refresh();
  };

  const remove = async (entry: EntryInfo) => {
    const ok = await confirmAction({
      title: `Delete ${isDir(entry) ? "folder" : "file"}?`,
      body: (
        <p>
          <span className="mono">{entry.path}</span>
          {isDir(entry) ? " and everything inside it" : ""} will be deleted.
        </p>
      ),
      action: "Delete",
      danger: true,
    });
    if (ok && (await attempt(`Deleted ${entry.name}`, () => files.remove(target, entry.path))))
      listing.refresh();
  };

  const onDrop = (event: DragEvent) => {
    event.preventDefault();
    setOver(false);
    if (event.dataTransfer.files.length) void upload(event.dataTransfer.files);
  };

  const crumbs = (dir ?? "/").split("/").filter(Boolean);

  return (
    <Card flush>
      <div className="toolbar" style={{ paddingTop: 14 }}>
        <Button
          variant="ghost"
          size="sm"
          icon="up"
          aria-label="Parent folder"
          disabled={!dir || dir === "/"}
          onClick={() => dir && go(parent(dir))}
        />
        <nav className="crumbs" aria-label="Path">
          <button type="button" className="crumbs__item" onClick={() => go("/")}>
            /
          </button>
          {crumbs.map((part, index) => {
            const path = `/${crumbs.slice(0, index + 1).join("/")}`;
            return (
              <span key={path} style={{ display: "contents" }}>
                {index > 0 && <span className="crumbs__sep">/</span>}
                <button
                  type="button"
                  className="crumbs__item"
                  aria-current={index === crumbs.length - 1 ? "location" : undefined}
                  onClick={() => go(path)}
                >
                  {part}
                </button>
              </span>
            );
          })}
        </nav>
        <span className="topbar__spacer" />
        {home && dir !== home && (
          <Button variant="ghost" size="sm" onClick={() => go(home)}>
            Home
          </Button>
        )}
        <Button variant="ghost" size="sm" icon="folderPlus" onClick={() => void makeDir()}>
          New Folder
        </Button>
        <Button size="sm" icon="upload" onClick={() => picker.current?.click()}>
          Upload
        </Button>
        <input
          ref={picker}
          type="file"
          multiple
          hidden
          onChange={(event) => {
            if (event.currentTarget.files) void upload(event.currentTarget.files);
            event.currentTarget.value = "";
          }}
        />
        <Button
          variant="ghost"
          size="sm"
          icon="refresh"
          aria-label="Refresh"
          onClick={listing.refresh}
        />
      </div>

      {uploads.length > 0 && (
        <div className="progress" aria-live="polite">
          {uploads.map((u) => (
            <div key={u.name}>
              Uploading <span className="mono">{u.name}</span> · {fmt.pct(u.ratio)}
            </div>
          ))}
        </div>
      )}
      <div style={{ padding: "0 18px" }}>
        <ErrorNote error={listing.error} />
      </div>

      <section
        className="dropzone"
        data-over={over}
        aria-label="File list; drop files here to upload"
        onDragOver={(event) => {
          event.preventDefault();
          setOver(true);
        }}
        onDragLeave={() => setOver(false)}
        onDrop={onDrop}
      >
        {entries.length === 0 ? (
          <Empty title={listing.loading || !dir ? "Loading…" : "This folder is empty"}>
            {!listing.loading && dir && "Drop files here or use Upload."}
          </Empty>
        ) : (
          <div className="table-wrap">
            <table className="table">
              <thead>
                <tr>
                  <th>Name</th>
                  <th className="right">Size</th>
                  <th>Permissions</th>
                  <th>Owner</th>
                  <th className="right">Modified</th>
                  <th className="shrink">
                    <span className="visually-hidden">Actions</span>
                  </th>
                </tr>
              </thead>
              <tbody>
                {entries.map((entry) => (
                  <tr key={entry.path}>
                    <td>
                      <button
                        type="button"
                        className={`file-name${isDir(entry) ? " file-name--dir" : ""}`}
                        onClick={() => void open(entry)}
                      >
                        <Icon
                          name={
                            isDir(entry)
                              ? "folder"
                              : entry.type === "FILE_TYPE_SYMLINK"
                                ? "link"
                                : "file"
                          }
                        />
                        <span className="file-name__text truncate">{entry.name}</span>
                      </button>
                      {entry.symlinkTarget && (
                        <div className="cell-sub mono">→ {entry.symlinkTarget}</div>
                      )}
                    </td>
                    <td className="right num muted">
                      {isDir(entry) ? "—" : fmt.bytes(Number(entry.size ?? 0))}
                    </td>
                    <td className="mono muted">{entry.permissions}</td>
                    <td className="muted">
                      {entry.owner}
                      {entry.group && entry.group !== entry.owner ? `:${entry.group}` : ""}
                    </td>
                    <td
                      className="right num muted"
                      title={entry.modifiedTime ? fmt.dateTime(entry.modifiedTime) : undefined}
                    >
                      {entry.modifiedTime ? fmt.ago(entry.modifiedTime) : "—"}
                    </td>
                    <td className="shrink">
                      <Menu
                        label={`Actions for ${entry.name}`}
                        items={[
                          ...(isDir(entry)
                            ? []
                            : [
                                {
                                  label: "Download",
                                  icon: "download" as const,
                                  onSelect: () => download(entry.path, entry.name),
                                },
                              ]),
                          { label: "Rename", icon: "rename", onSelect: () => void rename(entry) },
                          {
                            label: "Delete",
                            icon: "trash",
                            danger: true,
                            onSelect: () => void remove(entry),
                          },
                        ]}
                      />
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>

      <Modal
        title={preview?.name ?? ""}
        open={preview !== null}
        onClose={closePreview}
        wide
        footer={
          preview && (
            <>
              <span className="faint mono truncate" style={{ marginRight: "auto" }}>
                {preview.path}
              </span>
              <Button icon="download" onClick={() => download(preview.path, preview.name)}>
                Download
              </Button>
            </>
          )
        }
      >
        {preview?.image ? (
          <img
            src={preview.image}
            alt={preview.name}
            style={{ maxWidth: "100%", maxHeight: "60dvh" }}
          />
        ) : preview?.binary ? (
          <p>This looks like a binary file. Download it to inspect it locally.</p>
        ) : (
          <>
            <pre className="preview">{preview?.text}</pre>
            {preview?.truncated && (
              <p className="field__hint">Showing the first {fmt.bytes(PREVIEW_BYTES)}.</p>
            )}
          </>
        )}
      </Modal>
    </Card>
  );
}
