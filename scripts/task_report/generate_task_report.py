#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""
Tạo báo cáo PDF + (tuỳ chọn) chụp màn hình Playwright.

Cài đặt (một lần, từ thư mục gốc repo):
  pip install -r scripts/task_report/requirements-report.txt
  playwright install chromium

Chạy (stack Docker nên đang chạy nếu cần ảnh):
  python -m scripts.task_report.generate_task_report --output-dir reports/output

Tham số: --skip-screenshots | --with-airflow | --output-dir
Biến môi trường: GRAFANA_USER, GRAFANA_PASSWORD, AIRFLOW_USER, AIRFLOW_PASSWORD, GRAFANA_URL, ...
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

# Thư mục gốc repo (…/Crypto-DataLakehouse-Project)
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from scripts.task_report.capture import default_urls_from_env
from scripts.task_report.pdf_report import build_pdf


def _load_existing_screenshots(dir_path: Path) -> list[tuple[Path, str]]:
    if not dir_path.is_dir():
        return []
    paths = sorted(dir_path.glob("*.png"), key=lambda p: p.name)
    return [(p, p.stem.replace("_", " ")) for p in paths]


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Báo cáo PDF + screenshot Playwright")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("reports") / "output",
        help="Thư mục chứa screenshots/ và task_report.pdf",
    )
    parser.add_argument(
        "--skip-screenshots",
        action="store_true",
        help="Không chạy Playwright; dùng ảnh .png có sẵn trong output-dir/screenshots nếu có",
    )
    parser.add_argument(
        "--with-airflow",
        action="store_true",
        help="Bật luồng chụp Airflow (login admin)",
    )
    parser.add_argument(
        "--pdf-name",
        default="task_report.pdf",
        help="Tên file PDF đầu ra",
    )
    args = parser.parse_args()

    out: Path = args.output_dir.resolve()
    shot_dir = out / "screenshots"
    shot_dir.mkdir(parents=True, exist_ok=True)

    items: list[tuple[Path, str]] = []
    if args.skip_screenshots:
        items = _load_existing_screenshots(shot_dir)
        logging.info("Bỏ qua Playwright; tìm thấy %d ảnh trong %s", len(items), shot_dir)
    else:
        from scripts.task_report.capture import run_capture

        urls = default_urls_from_env()
        g_user = os.environ.get("GRAFANA_USER", "admin")
        g_pass = os.environ.get("GRAFANA_PASSWORD", "admin")
        a_user = os.environ.get("AIRFLOW_USER", "admin")
        a_pass = os.environ.get("AIRFLOW_PASSWORD", "admin")
        items = run_capture(
            shot_dir,
            urls,
            g_user,
            g_pass,
            a_user,
            a_pass,
            with_airflow=args.with_airflow,
        )
        if not items:
            logging.warning("Không thu được ảnh nào — kiểm tra dịch vụ localhost và URL (env).")

    no_images_explanation: str | None = None
    if not items:
        if args.skip_screenshots:
            no_images_explanation = (
                "Chế độ --skip-screenshots: không có file .png trong thư mục screenshots. "
                "Chép ảnh vào đó hoặc chạy lại và bỏ cờ --skip-screenshots khi đã bật stack."
            )
        else:
            no_images_explanation = (
                "Đã chạy Playwright nhưng không lưu được ảnh (thường do dịch vụ chưa sẵn sàng, từ chối kết nối hoặc timeout). "
                "Compose có Trino 18080 (host), Spark 8082, Airflow 8888, Grafana 3000, Prometheus 9090; "
                "vẫn nên có ảnh từ Trino/Spark khi các dịch vụ đã lên. "
                "Kiểm tra: docker compose ps, đợi vài phút sau khi up, mở thử URL trên trình duyệt, xem dòng WARN trong log khi chạy script."
            )

    pdf_path = out / args.pdf_name
    build_pdf(
        pdf_path,
        items,
        extra_image_paths=None,
        no_images_explanation=no_images_explanation,
    )
    logging.info("Đã tạo %s", pdf_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
