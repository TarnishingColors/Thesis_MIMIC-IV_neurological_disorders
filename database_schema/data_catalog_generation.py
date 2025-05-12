import yaml
from atlassian import Confluence
import os


with open("database_schema/data_catalog.yml") as f:
    catalog = yaml.safe_load(f)

confluence = Confluence(
    url=f'https://{os.environ["CONFLUENCE_WORKSPACE_ADDRESS"]}.atlassian.net/wiki',
    username=os.environ["CONFLUENCE_USERNAME"],
    password=os.environ["CONFLUENCE_API_TOKEN"]
)

space = os.environ["CONFLUENCE_SPACE_KEY"]
parent_page_id = os.environ["CONFLUENCE_PARENT_PAGE_ID"]

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
