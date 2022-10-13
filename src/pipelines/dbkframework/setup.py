import setuptools
from datetime import datetime
from pathlib import Path

current_file = Path(__file__).absolute()
print(f'Current file path: {current_file}')
# current_file_folder = Path(os.getcwd())
current_file_folder = Path(__file__).parent.absolute()
print(f"Current folder: {current_file_folder}")

path_readme = current_file_folder.joinpath('documentation.md')
modules_root = current_file.parent.parent.parent.joinpath('modules')

import sys
sys.path.append(str(current_file.parent.parent.parent.joinpath('modules')))
from devmaint.docgenerator import create_adow_documentation

package_name = current_file_folder.stem


modules_to_use = ['dbkcore', 'dbkdev', 'acai_ml']
path_requirements = current_file_folder.joinpath('requirements.txt')
package_dir = {}
documentations = []

with open(path_requirements, "r") as fh:
    requirements = [l.strip() for l in fh.readlines()]

requirements = [rq for rq in requirements if (rq) and (rq.startswith('#') is False)]

packages = []

for module in modules_to_use:
    module_path = modules_root.joinpath(module)
    packages = packages + setuptools.find_namespace_packages(where=modules_root, include=[f'{module}*'])
    package_dir[module] = module_path
    doc = create_adow_documentation(str(module_path))
    documentations.append(doc)

documentation = '\n\n'.join(documentations)

with open(str(path_readme), 'w', encoding="utf-8") as out:
    out.write(documentation)

today = datetime.today()
#version = f'{today:%Y}{today:%m}{today:%d}_{today:%H}{today:%M}{today:%S}'

# Amendments
version = 1


setuptools.setup(
    name=package_name,
    version=version,
    author="Davide Fornelli",
    author_email="daforne@microsoft.com",
    description="Core library for logging and using proper base object",
    # long_description=documentation,
    long_description_content_type="text/markdown",
    packages=packages,
    package_dir=package_dir,
    install_requires=requirements,
    python_requires='>=3'
)
