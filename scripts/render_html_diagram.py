"""Screenshot a fixed-size HTML diagram to a 2x PNG. Usage: render_html_diagram.py in.html out.png"""
import pathlib
import sys

from playwright.sync_api import sync_playwright

src, out = pathlib.Path(sys.argv[1]).resolve(), sys.argv[2]
with sync_playwright() as p:
    browser = p.chromium.launch()
    page = browser.new_page(device_scale_factor=2)
    page.goto(src.as_uri())
    size = page.evaluate("({w: document.body.scrollWidth, h: document.body.scrollHeight})")
    page.set_viewport_size({"width": size["w"], "height": size["h"]})
    page.screenshot(path=out, full_page=True)
    browser.close()
