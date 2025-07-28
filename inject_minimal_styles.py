# Скрипт добавления стилей в html версии Jupyter ноутбуков

from bs4 import BeautifulSoup
from pathlib import Path
import sys

MINIMAL_CSS = """
<style>
body {
    max-width: 960px;
    margin: 2em auto;
    padding: 0 1em;
    font-family: sans-serif;
    line-height: 1.6;
    background-color: white;
    color: black;
}
table, .output_subarea {
    display: block;
    overflow-x: auto;
    white-space: nowrap;
}
.output_area {
    overflow-x: auto;
    margin-bottom: 1em;
}
.output_subarea {
    background-color: #f8f8f8;
    padding: 8px;
    border-radius: 6px;
}
</style>
"""

def inject_style(file_path: Path):
    try:
        with file_path.open(encoding="utf-8") as f:
            soup = BeautifulSoup(f, "html.parser")

        if not soup.head:
            print(f"⚠️  Skipping (no <head>): {file_path}")
            return

        # Удалим старые стили, если уже добавляли их (по желанию)
        # soup.head.find_all('style')  # можно очистить, если нужно

        style_tag = BeautifulSoup(MINIMAL_CSS, "html.parser")
        soup.head.append(style_tag)

        with file_path.open("w", encoding="utf-8") as f:
            f.write(str(soup))

        print(f"✅ Styled: {file_path}")
    except Exception as e:
        print(f"❌ Failed: {file_path} — {e}")

def main():
    if len(sys.argv) > 1:
        base_path = Path(sys.argv[1])
    else:
        base_path = Path.cwd()

    exclude = ["index.html"]
    html_files = [file for file in list(base_path.rglob("*.html")) if file not in exclude]

    if not html_files:
        print("ℹ️  No HTML files found.")
        return

    print(f"🔍 Found {len(html_files)} .html files in {base_path}\n")

    for file_path in html_files:
        inject_style(file_path)

if __name__ == "__main__":
    main()
