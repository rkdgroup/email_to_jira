"""
PDF text extraction with dual-library fallback: PyMuPDF (primary) → pdfminer.six (fallback).
"""

import logging
import os
import tempfile
from pathlib import Path

log = logging.getLogger(__name__)

PAGE_SEPARATOR = "\n\n--- PAGE BREAK ---\n\n"
MIN_TEXT_LENGTH = 50


def get_pdf_page_count(pdf_path: str) -> int:
    """Return the number of pages in a PDF."""
    try:
        import pymupdf
    except ImportError:
        import fitz as pymupdf
    doc = pymupdf.open(pdf_path)
    count = doc.page_count
    doc.close()
    return count


def split_pdf_into_pages(pdf_path: str) -> tuple[str, list[str]]:
    """
    Split a multi-page PDF into individual single-page files inside a temp directory.
    Returns (tmp_dir, [page_paths]) — caller must delete tmp_dir when done.
    Page files are named {original_stem}_page1.pdf, _page2.pdf, etc.
    """
    try:
        import pymupdf
    except ImportError:
        import fitz as pymupdf

    doc = pymupdf.open(pdf_path)
    stem = Path(pdf_path).stem
    tmp_dir = tempfile.mkdtemp(prefix="dslf_split_")
    page_paths = []
    for i in range(doc.page_count):
        page_file = os.path.join(tmp_dir, f"{stem}_page{i + 1}.pdf")
        single = pymupdf.open()
        single.insert_pdf(doc, from_page=i, to_page=i)
        single.save(page_file)
        single.close()
        page_paths.append(page_file)
    doc.close()
    return tmp_dir, page_paths


def _extract_pymupdf(pdf_path: str) -> str:
    """Extract text using PyMuPDF (fitz). Returns concatenated page text."""
    try:
        import pymupdf
    except ImportError:
        import fitz as pymupdf  # older import name

    doc = pymupdf.open(pdf_path)
    pages = []
    for page in doc:
        pages.append(page.get_text())
    doc.close()
    return PAGE_SEPARATOR.join(pages)


def _extract_pymupdf_markdown(pdf_path: str) -> str:
    """Extract text as markdown using pymupdf4llm (for Claude fallback path)."""
    try:
        import pymupdf4llm
        return pymupdf4llm.to_markdown(pdf_path)
    except ImportError:
        # Fall back to plain text if pymupdf4llm not installed
        return _extract_pymupdf(pdf_path)


def _extract_pdfminer(pdf_path: str) -> str:
    """Extract text using pdfminer.six. Returns full document text."""
    from pdfminer.high_level import extract_text
    return extract_text(pdf_path)


def extract_pdf_text(pdf_path: str, mode: str = "plain") -> str:
    """
    Extract full text from a PDF file.

    Args:
        pdf_path: Absolute or relative path to the PDF file.
        mode: "plain" (default) for rule-based parsers, "markdown" for Claude fallback.

    Returns:
        Extracted text string.
        Prefixed with "[ERROR:..." on total failure.
        Prefixed with "[WARNING:LOW_TEXT]" if text appears garbled/scanned (< 50 chars).
    """
    text = ""

    # Primary: PyMuPDF
    try:
        if mode == "markdown":
            text = _extract_pymupdf_markdown(pdf_path)
        else:
            text = _extract_pymupdf(pdf_path)
    except Exception as e:
        log.warning("PyMuPDF extraction failed: %s", e)

    # Fallback: pdfminer.six if PyMuPDF produced poor results
    if len(text.strip()) < MIN_TEXT_LENGTH:
        try:
            pdfminer_text = _extract_pdfminer(pdf_path)
            if len(pdfminer_text.strip()) > len(text.strip()):
                log.info("Using pdfminer.six fallback (better result)")
                text = pdfminer_text
        except Exception as e:
            log.warning("pdfminer.six fallback also failed: %s", e)

    # Total failure
    if not text.strip():
        return f"[ERROR:NO_TEXT] Could not extract any text from {pdf_path}"

    # Quality gate
    if len(text.strip()) < MIN_TEXT_LENGTH:
        return f"[WARNING:LOW_TEXT] {text}"

    return text
