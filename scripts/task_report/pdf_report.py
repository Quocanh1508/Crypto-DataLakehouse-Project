# -*- coding: utf-8 -*-
"""Tạo PDF (reportlab): tiêu đề, nội dung task, bảng port, ảnh theo thứ tự."""
from __future__ import annotations

import html
import logging
import os
import platform
from pathlib import Path
from typing import Iterable

from PIL import Image as PILImage
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm, inch
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (
    Image,
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

from .content import (
    INTRO,
    MANUAL_PLACEHOLDERS,
    PORT_ROWS,
    PORT_TABLE_HEADER,
    REPORT_TITLE,
    SECTIONS,
)

logger = logging.getLogger(__name__)

MAX_IMG_WIDTH_PT = 6.5 * inch


def _register_unicode_font() -> str:
    """Đăng ký font TTF hỗ trợ tiếng Việt; fallback Helvetica."""
    candidates: list[Path] = []
    windir = os.environ.get("WINDIR", r"C:\Windows")
    if platform.system() == "Windows":
        candidates.extend(
            [
                Path(windir) / "Fonts" / "arial.ttf",
                Path(windir) / "Fonts" / "times.ttf",
            ]
        )
    candidates.extend(
        [
            Path("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"),
            Path("/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf"),
        ]
    )
    for p in candidates:
        if p.is_file():
            try:
                pdfmetrics.registerFont(TTFont("ReportUnicode", str(p)))
                logger.info("PDF font: %s", p)
                return "ReportUnicode"
            except Exception as e:
                logger.warning("Could not load font %s: %s", p, e)
    return "Helvetica"


def _para_style(base: ParagraphStyle, font: str, size: int = 11) -> ParagraphStyle:
    return ParagraphStyle(
        name=base.name + "VN",
        parent=base,
        fontName=font,
        fontSize=size,
        leading=size + 4,
        encoding="utf-8",
    )


def _scale_image(path: Path, max_w_pt: float) -> tuple[float, float]:
    """Kích thước (width, height) đơn vị point cho reportlab Image."""
    with PILImage.open(path) as im:
        w_px, h_px = im.size
    w_pt = w_px * 72 / 96
    h_pt = h_px * 72 / 96
    if w_pt > max_w_pt and w_pt > 0:
        r = max_w_pt / w_pt
        w_pt *= r
        h_pt *= r
    return w_pt, h_pt


DEFAULT_NO_IMAGES_TEXT = (
    "Không có ảnh được nhúng. Chạy lại script sau khi kiểm tra dịch vụ và thư mục screenshots."
)


def build_pdf(
    output_pdf: Path,
    screenshot_items: Iterable[tuple[Path, str]],
    extra_image_paths: list[Path] | None = None,
    *,
    no_images_explanation: str | None = None,
) -> None:
    """
    screenshot_items: (path, caption) theo thứ tự đã chụp.
    extra_image_paths: thêm ảnh chỉ đường dẫn (sort theo tên file), caption = tên file.
    no_images_explanation: đoạn tiếng Việt hiển thị khi không có ảnh (giải thích đúng ngữ cảnh).
    """
    font = _register_unicode_font()
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        "TitleVN",
        parent=styles["Title"],
        fontName=font,
        fontSize=18,
        leading=22,
        spaceAfter=12,
    )
    h2_style = ParagraphStyle(
        "Heading2VN",
        parent=styles["Heading2"],
        fontName=font,
        fontSize=14,
        leading=18,
        spaceAfter=8,
        spaceBefore=12,
    )
    body = ParagraphStyle(
        "BodyVN",
        parent=styles["Normal"],
        fontName=font,
        fontSize=11,
        leading=14,
    )

    doc = SimpleDocTemplate(
        str(output_pdf),
        pagesize=A4,
        rightMargin=2 * cm,
        leftMargin=2 * cm,
        topMargin=2 * cm,
        bottomMargin=2 * cm,
    )
    story: list = []

    story.append(Paragraph(REPORT_TITLE.replace("&", "&amp;"), title_style))
    story.append(Spacer(1, 0.4 * cm))
    story.append(Paragraph(INTRO.replace("&", "&amp;"), body))
    story.append(Spacer(1, 0.5 * cm))

    for heading, text in SECTIONS:
        story.append(Paragraph(heading.replace("&", "&amp;"), h2_style))
        story.append(Paragraph(text.replace("&", "&amp;"), body))

    story.append(Paragraph("Bảng port (host)", h2_style))
    cell_style = ParagraphStyle(
        "TableCell",
        fontName=font,
        fontSize=9,
        leading=11,
    )
    head_style = ParagraphStyle(
        "TableHead",
        fontName=font,
        fontSize=9,
        leading=11,
        textColor=colors.whitesmoke,
    )

    def tc(text: str, style: ParagraphStyle = cell_style) -> Paragraph:
        return Paragraph(text.replace("&", "&amp;"), style)

    table_data = [
        [tc(PORT_TABLE_HEADER[0], head_style), tc(PORT_TABLE_HEADER[1], head_style)],
    ]
    for a, b in PORT_ROWS:
        table_data.append([tc(a), tc(b)])
    t = Table(table_data, colWidths=[8 * cm, 4 * cm])
    t.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#4472C4")),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ]
        )
    )
    story.append(t)
    story.append(Spacer(1, 0.6 * cm))

    story.append(Paragraph("Minh chứng thủ công / placeholder", h2_style))
    story.append(Paragraph(MANUAL_PLACEHOLDERS.replace("&", "&amp;"), body))
    story.append(PageBreak())

    story.append(Paragraph("Ảnh chụp màn hình (luồng thao tác)", h2_style))
    story.append(Spacer(1, 0.3 * cm))

    items = list(screenshot_items)
    if extra_image_paths:
        for p in sorted(extra_image_paths, key=lambda x: x.name):
            items.append((p, p.name))

    if not items:
        explain = (no_images_explanation or DEFAULT_NO_IMAGES_TEXT).replace("&", "&amp;")
        story.append(Paragraph(explain, body))
    else:
        for img_path, caption in items:
            p = Path(img_path)
            if not p.is_file():
                logger.warning("Missing image: %s", p)
                continue
            story.append(Paragraph(f"<b>{caption.replace('&', '&amp;')}</b>", body))
            try:
                w, h = _scale_image(p, MAX_IMG_WIDTH_PT)
                story.append(Image(str(p), width=w, height=h))
            except Exception as e:
                logger.warning("Could not embed %s: %s", p, e)
                story.append(Paragraph(f"(Lỗi nhúng ảnh: {p.name})", body))
            story.append(Spacer(1, 0.4 * cm))

    doc.build(story)


def _teammate2_esc(s: str) -> str:
    return html.escape(s, quote=False)


def _teammate2_horizontal_rule(width_pt: float, color: colors.Color) -> Table:
    t = Table([[""]], colWidths=[width_pt], rowHeights=[2])
    t.setStyle(TableStyle([("BACKGROUND", (0, 0), (-1, -1), color)]))
    return t


def _merge_wrapped_lines(lines: list[str]) -> list[str]:
    """Gộp dòng tiếp nối (thụt đầu dòng) vào dòng trước để không bị đứt câu trong PDF."""
    import re

    if not lines:
        return []
    out: list[str] = []
    for line in lines:
        s = line.strip()
        if re.match(r"^={20,}$", s) or re.match(r"^-{20,}$", s):
            out.append(line)
            continue
        if line.startswith("  - ") or line.startswith("   - "):
            out.append(line)
            continue
        if (
            out
            and line.startswith("  ")
            and not s.startswith("•")
            and not (s.startswith("PHẦN ") and "—" in s)
            and not (re.match(r"^D\.\d+\s+—", s))
            and not re.match(r"^\d+\)", s)
        ):
            out[-1] = out[-1].rstrip() + " " + s
        else:
            out.append(line)
    return out


def build_teammate2_detail_pdf(output_pdf: Path) -> None:
    """
    Xuất PDF toàn bộ nội dung chi tiết Teammate 2 — định dạng có cấu trúc:
    trang bìa, tiêu đề phần, tiêu đề nhánh D.x, bullet, bảng port cuối.
    """
    import re

    from .content import PORT_ROWS, PORT_TABLE_HEADER
    from .teammate2_detailed_log import build_detailed_log_text

    text = build_detailed_log_text()
    font = _register_unicode_font()
    styles = getSampleStyleSheet()

    accent = colors.HexColor("#2563eb")
    accent_dark = colors.HexColor("#1e3a5f")
    muted = colors.HexColor("#64748b")

    cover_title = ParagraphStyle(
        "T2CoverTitle",
        parent=styles["Title"],
        fontName=font,
        fontSize=20,
        leading=26,
        spaceAfter=8,
        textColor=accent_dark,
        alignment=1,
    )
    cover_sub = ParagraphStyle(
        "T2CoverSub",
        parent=styles["Normal"],
        fontName=font,
        fontSize=11,
        leading=14,
        spaceAfter=16,
        textColor=muted,
        alignment=1,
    )
    meta = ParagraphStyle(
        "T2Meta",
        parent=styles["Normal"],
        fontName=font,
        fontSize=9,
        leading=12,
        textColor=muted,
        alignment=1,
    )

    section_style = ParagraphStyle(
        "T2Section",
        parent=styles["Heading2"],
        fontName=font,
        fontSize=13,
        leading=17,
        spaceBefore=16,
        spaceAfter=8,
        textColor=accent,
        borderPadding=4,
    )
    subsection_style = ParagraphStyle(
        "T2Subsection",
        parent=styles["Heading3"],
        fontName=font,
        fontSize=10.5,
        leading=14,
        spaceBefore=10,
        spaceAfter=5,
        textColor=accent_dark,
        leftIndent=0,
    )
    body = ParagraphStyle(
        "T2Body",
        parent=styles["Normal"],
        fontName=font,
        fontSize=9.5,
        leading=13,
        spaceAfter=5,
        alignment=0,
    )
    bullet_style = ParagraphStyle(
        "T2Bullet",
        parent=body,
        leftIndent=16,
        firstLineIndent=-12,
        bulletIndent=0,
        spaceAfter=4,
    )
    subbullet_style = ParagraphStyle(
        "T2Subbullet",
        parent=body,
        leftIndent=28,
        firstLineIndent=-10,
        spaceAfter=3,
        fontSize=9,
        leading=12,
        textColor=colors.HexColor("#334155"),
    )
    num_style = ParagraphStyle(
        "T2Numbered",
        parent=body,
        leftIndent=12,
        firstLineIndent=0,
        spaceBefore=4,
        spaceAfter=3,
    )

    output_pdf.parent.mkdir(parents=True, exist_ok=True)
    usable_w = A4[0] - 3 * cm
    doc = SimpleDocTemplate(
        str(output_pdf),
        pagesize=A4,
        rightMargin=1.5 * cm,
        leftMargin=1.5 * cm,
        topMargin=1.5 * cm,
        bottomMargin=1.5 * cm,
    )
    story: list = []

    # --- Trang bìa (từ khối đầu file log) ---
    lines = text.splitlines()
    cover_lines: list[str] = []
    i = 0
    while i < len(lines):
        ln = lines[i]
        if re.match(r"^={20,}$", ln.strip()):
            i += 1
            continue
        if "BẢN GHI CHI TIẾT" in ln or "TEAMMATE 2" in ln.upper():
            cover_lines.append(ln.strip())
            i += 1
            if i < len(lines) and lines[i].strip().startswith("Focus:"):
                cover_lines.append(lines[i].strip())
                i += 1
            break
        i += 1

    story.append(Spacer(1, 1.2 * cm))
    if cover_lines:
        story.append(Paragraph(_teammate2_esc(cover_lines[0]), cover_title))
        if len(cover_lines) > 1:
            story.append(Paragraph(_teammate2_esc(cover_lines[1]), cover_sub))
    else:
        story.append(
            Paragraph(
                _teammate2_esc("Báo cáo chi tiết — Teammate 2: Platform Reliability"),
                cover_title,
            )
        )
    story.append(_teammate2_horizontal_rule(usable_w, accent))
    story.append(Spacer(1, 0.4 * cm))
    story.append(
        Paragraph(
            _teammate2_esc("Cloud migration · HMS/Trino GCS · Prometheus / Grafana · Kafka & Spark JMX"),
            meta,
        )
    )
    story.append(Spacer(1, 1 * cm))

    # --- Nội dung (bỏ khối tiêu đề đã dùng cho bìa) ---
    if i >= len(lines):
        i = 0
    content_lines = _merge_wrapped_lines(lines[i:])

    body_buf: list[str] = []

    def flush_body_buf() -> None:
        nonlocal body_buf
        if body_buf:
            merged = [b.strip() for b in body_buf if b.strip()]
            if merged:
                story.append(Paragraph(_teammate2_esc(" ".join(merged)), body))
            body_buf = []

    for line in content_lines:
        raw = line.rstrip()
        s = raw.strip()

        if re.match(r"^={20,}$", s) or re.match(r"^-{20,}$", s):
            flush_body_buf()
            if re.match(r"^-{20,}$", s):
                story.append(Spacer(1, 0.35 * cm))
                story.append(_teammate2_horizontal_rule(usable_w, colors.HexColor("#e2e8f0")))
                story.append(Spacer(1, 0.35 * cm))
            continue

        if not s:
            flush_body_buf()
            story.append(Spacer(1, 0.15 * cm))
            continue

        if s.startswith("PHẦN ") and "—" in s:
            flush_body_buf()
            story.append(Paragraph(_teammate2_esc(s), section_style))
            continue

        if re.match(r"^D\.\d+\s+—", s):
            flush_body_buf()
            story.append(Paragraph(_teammate2_esc(s), subsection_style))
            continue

        if s.startswith("•"):
            flush_body_buf()
            story.append(Paragraph("• " + _teammate2_esc(s[1:].lstrip()), bullet_style))
            continue

        if raw.startswith("  - ") or raw.startswith("   - "):
            flush_body_buf()
            sub = re.sub(r"^-\s*", "", raw.strip())
            story.append(Paragraph("– " + _teammate2_esc(sub), subbullet_style))
            continue

        if re.match(r"^\d+\)", s):
            flush_body_buf()
            story.append(Paragraph(_teammate2_esc(s), num_style))
            continue

        body_buf.append(s)

    flush_body_buf()

    # --- Bảng port (chuẩn hóa, đẹp hơn bullet dài) ---
    story.append(PageBreak())
    story.append(Paragraph(_teammate2_esc("Phụ lục — Bảng port (host)"), section_style))
    story.append(Spacer(1, 0.3 * cm))

    cell_style = ParagraphStyle(
        "T2PortCell",
        fontName=font,
        fontSize=9,
        leading=11,
    )
    head_style = ParagraphStyle(
        "T2PortHead",
        fontName=font,
        fontSize=9,
        leading=11,
        textColor=colors.whitesmoke,
    )

    def tc(txt: str, st: ParagraphStyle) -> Paragraph:
        return Paragraph(_teammate2_esc(txt), st)

    port_data = [
        [tc(PORT_TABLE_HEADER[0], head_style), tc(PORT_TABLE_HEADER[1], head_style)],
    ]
    for a, b in PORT_ROWS:
        port_data.append([tc(a, cell_style), tc(b, cell_style)])

    port_tbl = Table(port_data, colWidths=[9.5 * cm, 5.5 * cm])
    port_tbl.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#2563eb")),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#cbd5e1")),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f8fafc")]),
                ("TOPPADDING", (0, 0), (-1, -1), 6),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
            ]
        )
    )
    story.append(port_tbl)
    story.append(Spacer(1, 0.5 * cm))
    story.append(
        Paragraph(
            _teammate2_esc("Ghi chú: broker Kafka trong Docker thường là kafka:29092."),
            ParagraphStyle("T2Foot", parent=body, fontSize=8.5, textColor=muted),
        )
    )

    doc.build(story)
    logger.info("Đã tạo PDF Teammate 2: %s", output_pdf)
