/**
 * <meta-service-menu> — the neighbouring meta-* services, as a burger menu.
 *
 * Replaces the eight divergent "services bar" implementations (four copies of
 * a React `ServiceNav.tsx`, two mirrored `services.js`, two bespoke inline
 * ones) with a single framework-free custom element. It drops into the React
 * dashboards as a plain tag — no React dependency — and into the vanilla UIs
 * as an ES module.
 *
 * Data comes from the host service's own `GET /api/neighbors`, same-origin.
 * Never cross-origin: the dev stack's self-signed certs and the OIDC redirects
 * make a fetch to a neighbour a dead end. Links are plain navigations.
 *
 * ⚠ MIRRORED FILE. Byte-identical copies live in every meta-* UI tree; see
 * `scripts/check-mirrors.sh`. Edit one → edit all.
 *
 * Shadow DOM is not optional here. The eight UIs use four incompatible token
 * vocabularies, and two of them define *different colours under the same
 * names* (`--bg-secondary` is #12121a in meta-sort, #16213e in meta-core,
 * #161b22 in meta-dup). meta-search and meta-share are also the only
 * light-mode-capable UIs, so a hardcoded dark palette would break them.
 * The element therefore styles itself from four tokens with dark defaults:
 *
 *     --mm-nav-fg, --mm-nav-bg, --mm-nav-border, --mm-nav-accent
 *
 * Each host maps its own variables onto those, e.g.
 *     meta-service-menu { --mm-nav-bg: var(--bg-secondary); }
 *
 * Attributes:
 *   current   name of the host service, so it can be marked and not linked
 *   endpoint  override the default "/api/neighbors"
 *   label     button tooltip (default "Services")
 *
 * Property:
 *   rewriteUrl  optional (name, url, neighbour) => url, for hosts that must
 *               translate internal hostnames into browser-reachable ones
 *               (meta-gateway does this for its dev/prod split).
 */

const ICONS = {
  "meta-core": "⚙️",
  "meta-sort": "📁",
  "meta-fuse": "🗂️",
  "meta-stremio": "🎬",
  "meta-dup": "🔍",
  "meta-search": "🔎",
  "meta-share": "🔗",
  "meta-gateway": "🌉",
  "meta-watch": "📺",
  "meta-listen": "🎧",
  "meta-read": "📖",
};
const DEFAULT_ICON = "📦";
const POLL_MS = 30000;

function titleCase(name) {
  return String(name || "")
    .split("-")
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
    .join(" ");
}

const TEMPLATE = `
<style>
  :host {
    --_fg: var(--mm-nav-fg, #e8eaed);
    --_bg: var(--mm-nav-bg, #1a1a2e);
    --_border: var(--mm-nav-border, rgba(255, 255, 255, 0.14));
    --_accent: var(--mm-nav-accent, #4ecdc4);
    position: relative;
    display: inline-block;
    font-family: inherit;
    font-size: 14px;
    line-height: 1.4;
  }
  button.trigger {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    gap: 0.4rem;
    width: 2.1rem;
    height: 2.1rem;
    padding: 0;
    cursor: pointer;
    color: var(--_fg);
    background: transparent;
    border: 1px solid var(--_border);
    border-radius: 8px;
    font: inherit;
    transition: border-color 0.15s, background 0.15s;
  }
  button.trigger:hover { border-color: var(--_accent); }
  button.trigger:focus-visible {
    outline: 2px solid var(--_accent);
    outline-offset: 2px;
  }
  button.trigger[aria-expanded="true"] { border-color: var(--_accent); }
  .bars { display: block; width: 15px; }
  .bars span {
    display: block;
    height: 2px;
    margin: 3px 0;
    border-radius: 2px;
    background: currentColor;
  }
  .panel {
    position: absolute;
    top: calc(100% + 6px);
    right: 0;
    z-index: 1000;
    min-width: 13rem;
    max-height: 70vh;
    overflow-y: auto;
    padding: 0.3rem;
    background: var(--_bg);
    border: 1px solid var(--_border);
    border-radius: 10px;
    box-shadow: 0 10px 30px rgba(0, 0, 0, 0.35);
  }
  .panel[hidden] { display: none; }
  a.item, .item {
    display: flex;
    align-items: center;
    gap: 0.55rem;
    padding: 0.45rem 0.6rem;
    border-radius: 6px;
    color: var(--_fg);
    text-decoration: none;
    white-space: nowrap;
  }
  a.item:hover { background: color-mix(in srgb, var(--_accent) 18%, transparent); }
  .item.active { color: var(--_accent); font-weight: 600; cursor: default; }
  .item.dead { opacity: 0.55; cursor: not-allowed; }
  .icon { font-size: 1rem; line-height: 1; }
  .dot {
    width: 6px;
    height: 6px;
    margin-left: auto;
    border-radius: 50%;
    background: var(--_accent);
  }
  .empty {
    padding: 0.6rem;
    opacity: 0.7;
    font-size: 0.85em;
    white-space: nowrap;
  }
  /* Respect a host that opts out of motion. */
  @media (prefers-reduced-motion: reduce) {
    button.trigger { transition: none; }
  }
</style>
<button class="trigger" type="button" aria-haspopup="menu" aria-expanded="false">
  <span class="bars" aria-hidden="true"><span></span><span></span><span></span></span>
</button>
<div class="panel" role="menu" hidden></div>
`;

class MetaServiceMenu extends HTMLElement {
  constructor() {
    super();
    this.attachShadow({ mode: "open" });
    this.shadowRoot.innerHTML = TEMPLATE;
    this._btn = this.shadowRoot.querySelector("button.trigger");
    this._panel = this.shadowRoot.querySelector(".panel");
    this._neighbors = [];
    this._timer = null;
    this._open = false;

    this._onDocClick = (e) => {
      if (!this._open) return;
      if (e.composedPath().includes(this)) return;
      this._setOpen(false);
    };
    this._onKey = (e) => {
      if (e.key === "Escape" && this._open) {
        this._setOpen(false);
        this._btn.focus();
      }
    };
  }

  connectedCallback() {
    this._btn.title = this.getAttribute("label") || "Services";
    this._btn.setAttribute("aria-label", this._btn.title);
    this._btn.addEventListener("click", () => this._setOpen(!this._open));
    document.addEventListener("click", this._onDocClick);
    document.addEventListener("keydown", this._onKey);

    this.refresh();
    this._timer = setInterval(() => this.refresh(), POLL_MS);
  }

  disconnectedCallback() {
    if (this._timer) clearInterval(this._timer);
    this._timer = null;
    document.removeEventListener("click", this._onDocClick);
    document.removeEventListener("keydown", this._onKey);
  }

  get endpoint() {
    return this.getAttribute("endpoint") || "/api/neighbors";
  }

  get current() {
    return this.getAttribute("current") || "";
  }

  async refresh() {
    try {
      const res = await fetch(this.endpoint, {
        headers: { Accept: "application/json" },
      });
      if (!res.ok) throw new Error(String(res.status));
      const data = await res.json();
      // `services` is the legacy alias some backends still emit.
      this._neighbors = data.neighbors || data.services || [];
    } catch {
      // Silent: the menu is navigation, not a health indicator. Keep whatever
      // we last had rather than blanking the list on one failed poll.
    }
    this._render();
  }

  _setOpen(open) {
    this._open = open;
    this._panel.hidden = !open;
    this._btn.setAttribute("aria-expanded", String(open));
    if (open) this.refresh();
  }

  _render() {
    const panel = this._panel;
    panel.textContent = "";

    const seen = new Set();
    const rows = [];
    for (const n of this._neighbors) {
      const name = n.name || "";
      if (!name || seen.has(name)) continue;
      seen.add(name);
      rows.push(n);
    }
    // Always show the host itself, even before its own echo comes back.
    if (this.current && !seen.has(this.current)) {
      rows.push({ name: this.current, baseUrl: "" });
    }
    rows.sort((a, b) => String(a.name).localeCompare(String(b.name)));

    if (rows.length === 0) {
      const empty = document.createElement("div");
      empty.className = "empty";
      empty.textContent = "No services found";
      panel.appendChild(empty);
      return;
    }

    for (const n of rows) {
      const isActive = n.name === this.current;
      let url = n.baseUrl || "";
      if (typeof this.rewriteUrl === "function") {
        try {
          url = this.rewriteUrl(n.name, url, n) || "";
        } catch {
          /* a bad hook must not blank the menu */
        }
      }

      // A neighbour with no reachable URL is shown but not linked — better
      // than a link that goes nowhere.
      const el = document.createElement(isActive || !url ? "span" : "a");
      el.className =
        "item" + (isActive ? " active" : "") + (!isActive && !url ? " dead" : "");
      if (el.tagName === "A") {
        el.href = url;
        el.setAttribute("role", "menuitem");
      }
      if (!isActive && !url) el.title = "No reachable URL announced";

      const icon = document.createElement("span");
      icon.className = "icon";
      icon.textContent = ICONS[n.name] || DEFAULT_ICON;

      const label = document.createElement("span");
      label.textContent = titleCase(n.name);

      el.append(icon, label);
      if (n.status === "running" || isActive) {
        const dot = document.createElement("span");
        dot.className = "dot";
        dot.title = n.status || "running";
        el.appendChild(dot);
      }
      panel.appendChild(el);
    }
  }
}

if (!customElements.get("meta-service-menu")) {
  customElements.define("meta-service-menu", MetaServiceMenu);
}

export default MetaServiceMenu;
