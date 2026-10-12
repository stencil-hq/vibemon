// History-API routing under /dashboard/: the driver answers every
// /dashboard/* path with index.html, so views deep-link and reload cleanly.

import { type AnchorHTMLAttributes, type MouseEvent, useSyncExternalStore } from "react";

export const BASE = "/dashboard";

/** Parsed location: path segments below `BASE` plus the query string. */
export interface Route {
  segments: string[];
  query: URLSearchParams;
}

const subscribers = new Set<() => void>();

function subscribe(listener: () => void): () => void {
  subscribers.add(listener);
  window.addEventListener("popstate", listener);
  return () => {
    subscribers.delete(listener);
    window.removeEventListener("popstate", listener);
  };
}

let cachedHref = "";
let cachedRoute: Route = { segments: [], query: new URLSearchParams() };

function snapshot(): Route {
  if (window.location.href !== cachedHref) {
    cachedHref = window.location.href;
    const rest = window.location.pathname.startsWith(BASE)
      ? window.location.pathname.slice(BASE.length)
      : window.location.pathname;
    cachedRoute = {
      segments: rest.split("/").filter(Boolean).map(decodeURIComponent),
      query: new URLSearchParams(window.location.search),
    };
  }
  return cachedRoute;
}

export function useRoute(): Route {
  return useSyncExternalStore(subscribe, snapshot);
}

/** Absolute URL of a dashboard path such as `/sandboxes/abc?tab=files`. */
export function href(path: string): string {
  return `${BASE}${path.startsWith("/") ? path : `/${path}`}`;
}

/** Pushes (or replaces) a dashboard path and re-renders route consumers. */
export function navigate(path: string, options: { replace?: boolean } = {}): void {
  const url = href(path);
  if (options.replace) window.history.replaceState(null, "", url);
  else window.history.pushState(null, "", url);
  for (const listener of subscribers) listener();
}

/**
 * Sets one query parameter of the current location. Filters replace the
 * history entry; `push` adds one (folder navigation, so Back works).
 */
export function setQuery(
  key: string,
  value: string | null,
  options: { push?: boolean } = {},
): void {
  const params = new URLSearchParams(window.location.search);
  if (value === null || value === "") params.delete(key);
  else params.set(key, value);
  const search = params.toString();
  const path = window.location.pathname.slice(BASE.length) || "/";
  navigate(search ? `${path}?${search}` : path, { replace: !options.push });
}

type LinkProps = Omit<AnchorHTMLAttributes<HTMLAnchorElement>, "href"> & { to: string };

/** In-app link: plain clicks navigate client-side; modified clicks open natively. */
export function Link({ to, onClick, ...rest }: LinkProps) {
  const handle = (event: MouseEvent<HTMLAnchorElement>) => {
    onClick?.(event);
    if (
      event.defaultPrevented ||
      event.button !== 0 ||
      event.metaKey ||
      event.ctrlKey ||
      event.shiftKey ||
      event.altKey
    ) {
      return;
    }
    event.preventDefault();
    navigate(to);
  };
  return <a href={href(to)} onClick={handle} {...rest} />;
}
