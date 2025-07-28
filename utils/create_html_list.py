import os
import pyperclip

def create_html_list(dir="docs", exclude=["index.html"]):
    docs_files = [file for file in os.listdir(dir) if file not in exclude]
    code_block = ""
    
    for file in docs_files:
        html_name = file
        name = file.split(".")[0]
        split_name = name.split("_")
        num = split_name.pop(0) + ". "
        clean_name = num + " ".join(split_name)
        one_line = f"""<li><a href="{html_name}" target="_blank">{clean_name}</a></li>\n"""
        code_block += one_line
    pyperclip.copy(code_block)

create_html_list()