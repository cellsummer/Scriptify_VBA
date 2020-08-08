from core import pull_vba
import unittest
import os


class TestCore(unittest.TestCase):
    def test_pull_vba(self):
        test_path = os.path.abspath(__file__)
        test_dir = os.path.split(test_path)[0]
        input_dir = os.path.join(test_dir, "Excel_Dir")
        output_dir = os.path.join(test_dir, "Excel_Dir")

        # create the vba project folders
        pull_vba.create_vba_projects(input_dir, output_dir)

        # test the generated file contents
        # sample/Mod1.vb
        with open(os.path.join(test_dir, "Excel_Dir/sample/Mod1.vb")) as rf:
            calc_resulst = rf.read()

        with open(os.path.join(test_dir, "Excel_Dir/test_sample/Mod1.vb")) as rf:
            correct_result = rf.read()

        self.assertEqual(calc_resulst, correct_result)

        # sample/sheet1.vb
        with open(os.path.join(test_dir, "Excel_Dir/sample/sheet1.vb")) as rf:
            calc_resulst = rf.read()

        with open(os.path.join(test_dir, "Excel_Dir/test_sample/sheet1.vb")) as rf:
            correct_result = rf.read()

        self.assertEqual(calc_resulst, correct_result)

