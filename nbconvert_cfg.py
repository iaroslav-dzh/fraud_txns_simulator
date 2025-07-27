import os

nbs_list = os.listdir("./notebooks_clean")
c = get_config()
c.NbConvertApp.notebooks = nbs_list
c.NbConvertApp.output_files_dir = "./docs"


jupyter nbconvert --output-dir='./docs' --to html notebooks_clean/*.ipynb