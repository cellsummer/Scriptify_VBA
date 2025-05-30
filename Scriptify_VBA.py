"""
Creating a simple GUI to scriptify VBA
"""

from gooey import Gooey, GooeyParser
from core.pull_vba import create_vba_projects


@Gooey(dump_build_config=True, program_name="Scriptify Excel VBA files")
def main():
    desc = "Convert VBA scripts embedded in Excels into folder structure"
    input_file_help_msg = "Specify your Excel directory"
    output_file_help_msg = "Specify your designated project folder"
    gui_parser = GooeyParser(description=desc)

    gui_parser.add_argument(
        "input", help=input_file_help_msg, widget="DirChooser"
    )
    gui_parser.add_argument(
        "output", help=output_file_help_msg, widget="DirChooser"
    )

    args = gui_parser.parse_args()
    # time.sleep(2)
    excel_dir = getattr(args, "Excel directory")
    output_dir = getattr(args, "Output directory")

    create_vba_projects(excel_dir, output_dir)
    # print(f"Asset inforce file: {getattr(args,'Asset inforce file')}")
    # print(f"Liability inforce file: {getattr(args,'Liability inforce file')}")


def here_is_more():
    pass


if __name__ == "__main__":
    main()
