from .docgenerator import generate_documentation
import argparse
from argparse import Namespace





def arguments() -> Namespace:
    parser = argparse.ArgumentParser(
        prog='docgen',
        description='Generates the documentation of the given package.'
    )
    parser.add_argument('-p', type=str, help='package parent folder')
    parser.add_argument('-n', type=str, help='package name')
    parser.add_argument('-o', default='documentation.md', help="output filename")
    args = parser.parse_args()
    return args


def main():
    """
    Main function that runs this script
    """

    args = arguments()

    package_parent_path = args.p.strip()
    package_name = args.n.strip()
    output_name = args.o.strip()

    outpath = generate_documentation(
        package_parent_path=package_parent_path,
        package_name=package_name,
        output_name=output_name
    )

    print(f'Documentation saved in "{outpath}"')
