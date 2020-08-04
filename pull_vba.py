import os
import sys
import glob
import win32com
from win32com.client import Dispatch


def main():
    scripts_dir = "C:\\Users\\aa\\Documents\\GitHub\\Test\\ExcelModels"
    output_dir = "C:\\Users\\aa\\Documents\\GitHub\\Test\\ExcelModels"
    create_vba_projects(scripts_dir, output_dir)


def create_vba_projects(scripts_dir, output_dir):
    """pull VBA code in the scripts folder to .vb files in the output directory"""
    sys.coinit_flags = 0  # comtypes.COINIT_MULTITHREADED

    # USE COMTYPES OR WIN32COM
    # com_instance = CreateObject("Excel.Application", dynamic = True) # USING COMTYPES
    com_instance = Dispatch("Excel.Application")  # USING WIN32COM
    com_instance.Visible = False
    com_instance.DisplayAlerts = False

    # currently only scans '.xlsm' files
    files = []
    for ext in ("*.xlsm", "*.xls", "*.xlsb"):
        files.extend(glob.glob(os.path.join(scripts_dir, ext)))

    for script_file in files:
        print(f"Processing: {script_file}")
        objworkbook = com_instance.Workbooks.Open(script_file)
        # create a folder for each workbook
        folder_name = os.path.join(output_dir, str(objworkbook.Name))
        folder_name = os.path.join(os.path.splitext(folder_name)[0], "")

        if not os.path.isdir(folder_name):
            # print(folder_name)
            os.makedirs(folder_name)

        for mod in objworkbook.VBProject.VBComponents:
            num_of_lines = mod.CodeModule.CountOfLines
            if num_of_lines > 0:
                script = mod.CodeModule.Lines(1, num_of_lines)
                mod_name = mod.Name
                file_name = os.path.join(folder_name, mod_name + ".vb")
                with open(file_name, "w") as f:
                    f.write(script)

        print(f"Processed: {script_file}")

    com_instance.Workbooks.Close()
    # print("instance closed.")
    com_instance.Quit()
    # print("instance quit.")
    return 0


if __name__ == "__main__":
    main()
