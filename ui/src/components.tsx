// Shared dashboard primitives: buttons, status, cards, meters, dialogs,
// menus, tabs, and the toast host.

import {
  type ButtonHTMLAttributes,
  type FormEvent,
  type ReactNode,
  useEffect,
  useId,
  useRef,
  useState,
  useSyncExternalStore,
} from "react";
import { createPortal } from "react-dom";
import { fmt } from "./format.ts";
import { dismissToast, useToasts } from "./hooks.ts";
import { Icon, type IconName } from "./icons.tsx";
import { Link } from "./router.tsx";

// ---------------------------------------------------------------- buttons

type Variant = "primary" | "secondary" | "ghost" | "danger" | "accent";

interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: Variant;
  size?: "sm" | "md";
  icon?: IconName;
  busy?: boolean;
}

export function Button({
  variant = "secondary",
  size = "md",
  icon,
  busy = false,
  className,
  children,
  disabled,
  type = "button",
  ...rest
}: ButtonProps) {
  return (
    <button
      type={type}
      className={`btn btn--${variant} btn--${size}${children ? "" : " btn--icon"}${className ? ` ${className}` : ""}`}
      disabled={disabled || busy}
      aria-busy={busy || undefined}
      {...rest}
    >
      {busy ? <span className="spinner" aria-hidden="true" /> : icon ? <Icon name={icon} /> : null}
      {children}
    </button>
  );
}

export function CopyButton({ value, label = "Copy" }: { value: string; label?: string }) {
  const [copied, setCopied] = useState(false);
  useEffect(() => {
    if (!copied) return;
    const timer = window.setTimeout(() => setCopied(false), 1500);
    return () => window.clearTimeout(timer);
  }, [copied]);
  return (
    <Button
      variant="ghost"
      size="sm"
      icon={copied ? "check" : "copy"}
      aria-label={copied ? "Copied" : label}
      title={label}
      onClick={() => {
        void navigator.clipboard?.writeText(value).then(() => setCopied(true));
      }}
    />
  );
}

// ----------------------------------------------------------------- status

export type Tone = "ok" | "warn" | "error" | "live" | "muted" | "info";

export function StatusDot({ tone, pulse = false }: { tone: Tone; pulse?: boolean }) {
  return <span className={`dot dot--${tone}${pulse ? " dot--pulse" : ""}`} aria-hidden="true" />;
}

/** Dot plus word; color is never the only signal. */
export function Status({ tone, children }: { tone: Tone; children: ReactNode }) {
  return (
    <span className="status">
      <StatusDot tone={tone} />
      {children}
    </span>
  );
}

export function Badge({ tone = "muted", children }: { tone?: Tone; children: ReactNode }) {
  return <span className={`badge badge--${tone}`}>{children}</span>;
}

// ------------------------------------------------------------------ cards

interface CardProps {
  title?: ReactNode;
  subtitle?: ReactNode;
  actions?: ReactNode;
  children: ReactNode;
  className?: string;
  flush?: boolean;
}

export function Card({ title, subtitle, actions, children, className, flush }: CardProps) {
  return (
    <section className={`card${flush ? " card--flush" : ""}${className ? ` ${className}` : ""}`}>
      {(title || actions) && (
        <header className="card__head">
          <div className="card__titles">
            {title && <h2 className="card__title">{title}</h2>}
            {subtitle && <p className="card__subtitle">{subtitle}</p>}
          </div>
          {actions && <div className="card__actions">{actions}</div>}
        </header>
      )}
      {children}
    </section>
  );
}

export function Stat({ label, value, sub }: { label: string; value: ReactNode; sub?: ReactNode }) {
  return (
    <div className="stat">
      <div className="stat__label">{label}</div>
      <div className="stat__value">{value}</div>
      {sub && <div className="stat__sub">{sub}</div>}
    </div>
  );
}

/** Horizontal usage bar; turns amber above 75 % and red above 90 %. */
export function Meter({ used, total, label }: { used: number; total: number; label: string }) {
  const ratio = total > 0 ? Math.min(1, used / total) : 0;
  const level = ratio > 0.9 ? "error" : ratio > 0.75 ? "warn" : "ok";
  return (
    <div className="meter">
      <div className="meter__track" role="img" aria-label={`${label}: ${fmt.pct(ratio)}`}>
        <div
          className={`meter__fill meter__fill--${level}`}
          style={{ transform: `scaleX(${ratio})` }}
        />
      </div>
    </div>
  );
}

/** One label/value pair inside a `<dl className="kv">` grid. */
export function Detail({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div>
      <dt className="kv__key">{label}</dt>
      <dd className="kv__value">{children}</dd>
    </div>
  );
}

export function Empty({ title, children }: { title: string; children?: ReactNode }) {
  return (
    <div className="empty">
      <p className="empty__title">{title}</p>
      {children && <div className="empty__body">{children}</div>}
    </div>
  );
}

export function ErrorNote({ error }: { error: Error | undefined }) {
  if (!error) return null;
  return (
    <p className="error-note" role="alert">
      {error.message}
    </p>
  );
}

// ---------------------------------------------------------------- dialogs

interface ModalProps {
  title: string;
  open: boolean;
  onClose: () => void;
  children: ReactNode;
  footer?: ReactNode;
  wide?: boolean;
}

/** Native modal `<dialog>`: focus containment, Escape, and top layer for free. */
export function Modal({ title, open, onClose, children, footer, wide }: ModalProps) {
  const ref = useRef<HTMLDialogElement>(null);
  const titleId = useId();
  useEffect(() => {
    const dialog = ref.current;
    if (!dialog) return;
    if (open && !dialog.open) dialog.showModal();
    if (!open && dialog.open) dialog.close();
  }, [open]);
  return (
    // biome-ignore lint/a11y/useKeyWithClickEvents: backdrop click mirrors Escape, which the native dialog handles
    <dialog
      ref={ref}
      className={`modal${wide ? " modal--wide" : ""}`}
      aria-labelledby={titleId}
      onClose={onClose}
      onCancel={(event) => {
        event.preventDefault();
        onClose();
      }}
      onClick={(event) => {
        if (event.target === event.currentTarget) onClose();
      }}
    >
      {open && (
        <div className="modal__panel">
          <header className="modal__head">
            <h2 id={titleId} className="modal__title">
              {title}
            </h2>
            <Button variant="ghost" size="sm" icon="close" aria-label="Close" onClick={onClose} />
          </header>
          <div className="modal__body">{children}</div>
          {footer && <footer className="modal__foot">{footer}</footer>}
        </div>
      )}
    </dialog>
  );
}

interface AskRequest {
  title: string;
  body?: ReactNode;
  action: string;
  danger?: boolean;
  /** Present for text prompts: the field label and its initial value. */
  input?: { label: string; initial: string };
  /** The entered text (`""` for confirmations), or `null` when dismissed. */
  resolve: (value: string | null) => void;
  id: number;
}

let pendingAsk: AskRequest | null = null;
let askSeq = 0;
const askListeners = new Set<() => void>();

function setAsk(next: AskRequest | null): void {
  pendingAsk = next;
  for (const listener of askListeners) listener();
}

function ask(request: Omit<AskRequest, "resolve" | "id">): Promise<string | null> {
  pendingAsk?.resolve(null);
  const { promise, resolve } = Promise.withResolvers<string | null>();
  askSeq += 1;
  setAsk({ ...request, resolve, id: askSeq });
  return promise;
}

/** Asks for confirmation in a modal; resolves `true` only on the action button. */
export async function confirmAction(request: {
  title: string;
  body: ReactNode;
  action: string;
  danger?: boolean;
}): Promise<boolean> {
  return (await ask(request)) !== null;
}

/** Asks for one line of text in a modal; `null` when cancelled or left blank. */
export async function promptText(request: {
  title: string;
  label: string;
  action: string;
  initial?: string;
}): Promise<string | null> {
  const value = await ask({
    title: request.title,
    action: request.action,
    input: { label: request.label, initial: request.initial ?? "" },
  });
  return value?.trim() || null;
}

/** Renders the pending confirmation or prompt; mounted once by the app. */
export function DialogHost() {
  const request = useSyncExternalStore(
    (listener) => {
      askListeners.add(listener);
      return () => askListeners.delete(listener);
    },
    () => pendingAsk,
  );
  const finish = (value: string | null) => {
    request?.resolve(value);
    setAsk(null);
  };
  return (
    <Modal
      title={request?.title ?? ""}
      open={request !== null}
      onClose={() => finish(null)}
      footer={
        <>
          <Button onClick={() => finish(null)}>Cancel</Button>
          <Button type="submit" form="ask-form" variant={request?.danger ? "danger" : "primary"}>
            {request?.action}
          </Button>
        </>
      }
    >
      {request && <AskForm key={request.id} request={request} onDone={finish} />}
    </Modal>
  );
}

/** Body of one dialog request; keyed per request so the field starts from its initial value. */
function AskForm({ request, onDone }: { request: AskRequest; onDone: (value: string) => void }) {
  const [text, setText] = useState(request.input?.initial ?? "");
  const submit = (event: FormEvent) => {
    event.preventDefault();
    onDone(request.input ? text : "");
  };
  return (
    <form id="ask-form" onSubmit={submit}>
      {request.body}
      {request.input && (
        <label className="field">
          <span className="field__label">{request.input.label}</span>
          <input
            className="input mono"
            name="value"
            autoComplete="off"
            spellCheck={false}
            value={text}
            onChange={(event) => setText(event.currentTarget.value)}
            onFocus={(event) => event.currentTarget.select()}
            // biome-ignore lint/a11y/noAutofocus: the dialog's only field
            autoFocus
          />
        </label>
      )}
    </form>
  );
}

// ------------------------------------------------------------------ menus

export interface MenuItem {
  label: string;
  icon?: IconName;
  danger?: boolean;
  disabled?: boolean;
  onSelect: () => void;
}

/** Fixed-position placement of an open menu: below the trigger, or above it near the bottom edge. */
interface MenuPlacement {
  right: number;
  top?: number;
  bottom?: number;
}

/**
 * Overflow menu: a ghost button that opens a list of actions. The list is
 * portaled to `<body>` with fixed positioning so scrolling tables and clipped
 * cards never cut it off; scrolling or resizing closes it.
 */
export function Menu({ label, items }: { label: string; items: MenuItem[] }) {
  const [placement, setPlacement] = useState<MenuPlacement | null>(null);
  const trigger = useRef<HTMLDivElement>(null);
  const list = useRef<HTMLDivElement>(null);
  const open = placement !== null;
  useEffect(() => {
    if (!open) return;
    const close = () => setPlacement(null);
    const onPointer = (event: PointerEvent) => {
      const target = event.target as Node;
      if (!trigger.current?.contains(target) && !list.current?.contains(target)) close();
    };
    const onKey = (event: KeyboardEvent) => {
      if (event.key === "Escape") close();
    };
    document.addEventListener("pointerdown", onPointer);
    document.addEventListener("keydown", onKey);
    window.addEventListener("scroll", close, true);
    window.addEventListener("resize", close);
    return () => {
      document.removeEventListener("pointerdown", onPointer);
      document.removeEventListener("keydown", onKey);
      window.removeEventListener("scroll", close, true);
      window.removeEventListener("resize", close);
    };
  }, [open]);
  const toggle = () => {
    const rect = trigger.current?.getBoundingClientRect();
    if (open || !rect) return setPlacement(null);
    const right = window.innerWidth - rect.right;
    const height = items.length * 32 + 8;
    setPlacement(
      rect.bottom + 6 + height > window.innerHeight
        ? { right, bottom: window.innerHeight - rect.top + 6 }
        : { right, top: rect.bottom + 6 },
    );
  };
  return (
    <div className="menu" ref={trigger}>
      <Button
        variant="ghost"
        size="sm"
        icon="more"
        aria-label={label}
        aria-haspopup="menu"
        aria-expanded={open}
        onClick={(event) => {
          event.stopPropagation();
          toggle();
        }}
      />
      {placement &&
        createPortal(
          <div className="menu__list" role="menu" ref={list} style={placement}>
            {items.map((item) => (
              <button
                key={item.label}
                type="button"
                role="menuitem"
                className={`menu__item${item.danger ? " menu__item--danger" : ""}`}
                disabled={item.disabled}
                onClick={(event) => {
                  event.stopPropagation();
                  setPlacement(null);
                  item.onSelect();
                }}
              >
                {item.icon && <Icon name={item.icon} />}
                {item.label}
              </button>
            ))}
          </div>,
          document.body,
        )}
    </div>
  );
}

// ------------------------------------------------------------ navigation

export interface Option<T extends string> {
  value: T;
  label: string;
}

/** Single-choice pill group (filters, ranges); toggle buttons, one pressed. */
export function Segmented<T extends string>({
  label,
  value,
  options,
  onChange,
}: {
  label: string;
  value: T;
  options: Option<T>[];
  onChange: (value: T) => void;
}) {
  return (
    <fieldset className="segmented">
      <legend className="visually-hidden">{label}</legend>
      {options.map((option) => (
        <button
          key={option.value}
          type="button"
          aria-pressed={option.value === value}
          className="segmented__item"
          onClick={() => onChange(option.value)}
        >
          {option.label}
        </button>
      ))}
    </fieldset>
  );
}

/** Tab strip of real links so tabs deep-link and open in new tabs. */
export function Tabs({
  tabs,
  active,
  base,
}: {
  tabs: { id: string; label: string; icon: IconName }[];
  active: string;
  base: string;
}) {
  return (
    <nav className="tabs" aria-label="Sections">
      {tabs.map((tab) => (
        <Link
          key={tab.id}
          to={`${base}?tab=${tab.id}`}
          className="tabs__item"
          aria-current={tab.id === active ? "page" : undefined}
        >
          <Icon name={tab.icon} />
          {tab.label}
        </Link>
      ))}
    </nav>
  );
}

export function Toaster() {
  const toasts = useToasts();
  return (
    <div className="toaster" aria-live="polite">
      {toasts.map((t) => (
        <div
          key={t.id}
          className={`toast toast--${t.tone}`}
          role={t.tone === "error" ? "alert" : "status"}
        >
          <StatusDot tone={t.tone === "error" ? "error" : "ok"} />
          <span className="toast__text">{t.message}</span>
          <button
            type="button"
            className="toast__close"
            aria-label="Dismiss"
            onClick={() => dismissToast(t.id)}
          >
            <Icon name="close" size={14} />
          </button>
        </div>
      ))}
    </div>
  );
}
