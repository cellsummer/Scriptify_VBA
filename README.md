# Scriptify_VBA
Convert the VBA code embedded in the macro-enabled Excel files into plain text files for version control.

[toc]

## Prerequisite

1. pywin32: to use win32com libraries.

   ```powershell
   pip install pywin32
   ```

2. gooey to use the simple user interface

   ```powershell
   pip install gooey
   ```

## Use a pre-built executable

Use the .exe file in the "dist" folder.

## Example

In the "Excel directory": we have 2 Excel files that we want to "scriptify":

1. sample.xlsm
2. sample_2.xlsb

After running the program, two folders will be created in the output directory: 

1. sample
2. sample_2

Two .vb files were created in folder "sample":

**Mod1.vb**: this is extracted from the module "Mod1" in the sample.xlsm

```vb
Sub test_macro_1()

    ' this is some comments

    MsgBox "hello world"

End Sub

Sub test_macro_2()

' this is another macro

MsgBox "this is just another piece of coding"

End Sub
```

**Sheet1.vb**: this is extracted from the worksheet "sheet1"  in the sample.xlsm

```vb
Sub test()

'this is a macro in the worksheet

MsgBox "this is called from the worksheet"

End Sub
```

