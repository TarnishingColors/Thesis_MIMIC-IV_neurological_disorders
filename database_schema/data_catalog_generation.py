import yaml
from atlassian import Confluence
import configparser


config = configparser.ConfigParser()
config.read('config.ini')

with open("database_schema/data_catalog.yml") as f:
    catalog = yaml.safe_load(f)

confluence = Confluence(
    url=f'https://{config["confluence"]["workspace_address"]}.atlassian.net/wiki',
    username=config["confluence"]["username"],
    password=config["confluence"]["api_token"]
)

space = config["confluence"]["space_name"]
parent_page_id = config["confluence"]["parent_page_id"]

for table in catalog['tables']:
    title = f"Table: {table['name']}"
    content = f"<h2>{table['description']}</h2><table><tr><th>Column</th><th>Type</th><th>Description</th></tr>"

    for col in table['columns']:
        content += f"<tr><td>{col['name']}</td><td>{col['type']}</td><td>{col['description']}</td></tr>"

    content += "</table>"

    existing = confluence.get_page_by_title(space, title)
    if existing:
        confluence.update_page(existing['id'], title, content)
    else:
        confluence.create_page(space, title, content, parent_id=parent_page_id)
