# -*- coding: utf-8 -*-
"""
Chụp màn hình luồng thao tác (Playwright sync API).
Mỗi bước lưu một PNG với tiền tố số thứ tự toàn cục + mô tả bước.
"""
from __future__ import annotations

import logging
import os
import re
from pathlib import Path
from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from playwright.sync_api import Browser, Page, Playwright

logger = logging.getLogger(__name__)

VIEWPORT = {"width": 1280, "height": 720}
NAV_TIMEOUT_MS = 20_000
STEP_WAIT_MS = 1_500


def _slug(label: str) -> str:
    s = re.sub(r"[^\w\-]+", "_", label.strip().lower(), flags=re.UNICODE)
    s = re.sub(r"_+", "_", s).strip("_")
    return s[:80] or "step"


class ScreenshotSink:
    def __init__(self, out_dir: Path) -> None:
        self.out_dir = Path(out_dir)
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.counter = 0

    def shot(self, page: Page, label: str) -> tuple[Path, str]:
        self.counter += 1
        slug = _slug(label)
        name = f"{self.counter:03d}_{slug}.png"
        path = self.out_dir / name
        page.screenshot(path=str(path), full_page=False)
        caption = f"{self.counter:03d} — {label}"
        logger.info("Saved %s", path.name)
        return path, caption


def _safe_goto(page: Page, url: str) -> bool:
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
        page.wait_for_timeout(STEP_WAIT_MS)
        return True
    except Exception as e:
        logger.warning("goto %s failed: %s", url, e)
        return False


def scenario_grafana(
    page: Page,
    sink: ScreenshotSink,
    base: str,
    user: str,
    password: str,
) -> list[tuple[Path, str]]:
    out: list[tuple[Path, str]] = []
    login_url = base.rstrip("/") + "/login"
    if not _safe_goto(page, login_url):
        return out
    out.append(sink.shot(page, "Grafana: trang đăng nhập"))

    try:
        user_input = page.locator('input[name="user"], input#username, input[type="text"]').first
        user_input.wait_for(state="visible", timeout=15_000)
        user_input.fill(user)
        page.locator('input[name="password"], input#password, input[type="password"]').first.fill(password)
        page.locator('button[type="submit"], button:has-text("Log in"), button:has-text("Login")').first.click()
        page.wait_for_load_state("domcontentloaded", timeout=30_000)
    except Exception as e:
        logger.warning("Grafana login: %s", e)
        return out
    page.wait_for_timeout(STEP_WAIT_MS)
    out.append(sink.shot(page, "Grafana: sau đăng nhập (home)"))

    explore = base.rstrip("/") + "/explore"
    if _safe_goto(page, explore):
        out.append(sink.shot(page, "Grafana: Explore"))

    ds = base.rstrip("/") + "/connections/datasources"
    if _safe_goto(page, ds):
        out.append(sink.shot(page, "Grafana: Data sources"))

    return out


def scenario_prometheus(page: Page, sink: ScreenshotSink, base: str) -> list[tuple[Path, str]]:
    out: list[tuple[Path, str]] = []
    graph = base.rstrip("/") + "/graph"
    if _safe_goto(page, graph):
        out.append(sink.shot(page, "Prometheus: Graph"))

    targets = base.rstrip("/") + "/targets"
    if _safe_goto(page, targets):
        out.append(sink.shot(page, "Prometheus: Status Targets"))

    return out


def _trino_fill_and_run(page: Page) -> None:
    q = "SHOW CATALOGS"
    for sel in (
        "textarea",
        "div.cm-content",
        "[role='textbox']",
        ".monaco-editor textarea",
    ):
        loc = page.locator(sel).first
        try:
            loc.wait_for(state="visible", timeout=3_000)
            loc.click(timeout=3_000)
            loc.fill(q, timeout=8_000)
            break
        except Exception:
            continue
    page.wait_for_timeout(500)
    try:
        page.keyboard.press("Control+Enter")
    except Exception:
        pass
    try:
        run_btn = page.get_by_role("button", name=re.compile(r"run|execute", re.I))
        if run_btn.count() > 0:
            run_btn.first.click(timeout=5_000)
    except Exception:
        pass
    page.wait_for_timeout(STEP_WAIT_MS)


def scenario_trino(page: Page, sink: ScreenshotSink, base: str) -> list[tuple[Path, str]]:
    out: list[tuple[Path, str]] = []
    # Thử root trước (nhiều bản Trino phục vụ UI tại /), sau đó /ui/
    for path in ("/", "/ui/", "/ui"):
        url = base.rstrip("/") + path
        if _safe_goto(page, url):
            break
    else:
        return out
    out.append(sink.shot(page, "Trino: UI (trước query)"))

    try:
        _trino_fill_and_run(page)
    except Exception as e:
        logger.warning("Trino query step: %s", e)
    out.append(sink.shot(page, "Trino: sau khi chạy SHOW CATALOGS"))

    return out


def scenario_spark(page: Page, sink: ScreenshotSink, base: str) -> list[tuple[Path, str]]:
    out: list[tuple[Path, str]] = []
    root = base.rstrip("/") + "/"
    if not _safe_goto(page, root):
        return out
    out.append(sink.shot(page, "Spark Master: overview"))

    workers_url = base.rstrip("/") + "/workers/"
    if _safe_goto(page, workers_url):
        out.append(sink.shot(page, "Spark Master: workers"))

    return out


def scenario_airflow(
    page: Page,
    sink: ScreenshotSink,
    base: str,
    user: str,
    password: str,
) -> list[tuple[Path, str]]:
    out: list[tuple[Path, str]] = []
    login = base.rstrip("/") + "/login/"
    if not _safe_goto(page, login):
        if not _safe_goto(page, base.rstrip("/") + "/"):
            return out
    out.append(sink.shot(page, "Airflow: trang đăng nhập"))

    try:
        page.locator("#username, input[name='username']").first.fill(user, timeout=10_000)
        page.locator("#password, input[name='password']").first.fill(password, timeout=10_000)
        page.locator('button[type="submit"], input[type="submit"]').first.click()
        page.wait_for_load_state("domcontentloaded", timeout=45_000)
    except Exception as e:
        logger.warning("Airflow login: %s", e)
        return out
    page.wait_for_timeout(STEP_WAIT_MS)
    out.append(sink.shot(page, "Airflow: danh sách DAG"))

    home = base.rstrip("/") + "/home"
    if _safe_goto(page, home):
        out.append(sink.shot(page, "Airflow: Home"))

    return out


def run_capture(
    output_screenshots_dir: Path,
    urls: dict[str, str],
    grafana_user: str,
    grafana_password: str,
    airflow_user: str,
    airflow_password: str,
    with_airflow: bool,
) -> list[tuple[Path, str]]:
    """
    Trả về danh sách (đường dẫn PNG, caption) theo thứ tự chụp.
    """
    from playwright.sync_api import Browser, Playwright, sync_playwright

    screenshots_dir = Path(output_screenshots_dir)
    screenshots_dir.mkdir(parents=True, exist_ok=True)
    results: list[tuple[Path, str]] = []

    def run(pw: Playwright) -> None:
        browser: Browser = pw.chromium.launch(headless=True)
        context = browser.new_context(viewport=VIEWPORT, ignore_https_errors=True)
        page = context.new_page()
        sink = ScreenshotSink(screenshots_dir)

        scenarios: list[tuple[str, Callable[[], list[tuple[Path, str]]]]] = [
            (
                "grafana",
                lambda: scenario_grafana(
                    page,
                    sink,
                    urls["grafana"],
                    grafana_user,
                    grafana_password,
                ),
            ),
            ("prometheus", lambda: scenario_prometheus(page, sink, urls["prometheus"])),
            ("trino", lambda: scenario_trino(page, sink, urls["trino"])),
            ("spark", lambda: scenario_spark(page, sink, urls["spark"])),
        ]
        if with_airflow:
            scenarios.append(
                (
                    "airflow",
                    lambda: scenario_airflow(
                        page,
                        sink,
                        urls["airflow"],
                        airflow_user,
                        airflow_password,
                    ),
                )
            )

        for name, fn in scenarios:
            try:
                batch = fn()
                results.extend(batch)
            except Exception as e:
                logger.warning("Scenario %s failed: %s", name, e)

        context.close()
        browser.close()

    with sync_playwright() as pw:
        run(pw)

    return results


def default_urls_from_env() -> dict[str, str]:
    host = os.environ.get("REPORT_HOST", "localhost")
    return {
        "grafana": os.environ.get("GRAFANA_URL", f"http://{host}:3000"),
        "prometheus": os.environ.get("PROMETHEUS_URL", f"http://{host}:9090"),
        "trino": os.environ.get(
            "TRINO_URL",
            f"http://{host}:{os.environ.get('TRINO_PORT', '18080')}",
        ),
        "spark": os.environ.get("SPARK_URL", f"http://{host}:8082"),
        "airflow": os.environ.get("AIRFLOW_URL", f"http://{host}:8888"),
    }
