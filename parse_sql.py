# This script is to parse the sql with placeholders
# The value of the placeholders will be in the top section
import re
import os
import typer


class MetaSql():
    def __init__(self, meta_file):
        self.meta_file = meta_file
        self.header = []
        self.body = []

        with open(self.meta_file, 'r') as f:
            self.lines = f.readlines()

        # remove starting and ending blank lines
        while str.strip(self.lines[0]) == '':
            self.lines.pop(0)

        while str.strip(self.lines[-1]) == '':
            self.lines.pop(-1)

    def get_header_and_body(self, ):
        if self.lines[0][:3] != '---':
            self.header = []
            self.body = self.lines
            return

        for i in range(1, len(self.lines)):
            if self.lines[i][:3] == '---':
                break
            else:
                self.header.append(self.lines[i])

        self.body = self.lines[i+1:]

        return

    def replace_placeholders(self,):
        vars = {}
        for line in self.header:
            kvp = line.split(":")
            vars[kvp[0].strip()] = kvp[1].strip()

        for i, line in enumerate(self.body):
            keys = re.findall(r'\{\{.*\}\}', line)
            for key in keys:
                var_key = key[2:-2]
                line = line.replace(key, vars[var_key])
            self.body[i] = line

        return

    def save_compiled_sql(self, ):
        fname = os.path.splitext(self.meta_file)[0]
        fext = os.path.splitext(self.meta_file)[1]

        compiled_fname = fname + "_compiled"

        with open(f'{compiled_fname}.{fext}', 'w') as f:
            f.writelines(self.body)

        return

    def compile(self, ):
        self.get_header_and_body()
        self.replace_placeholders()
        self.save_compiled_sql()


def main(meta_file):
    MetaSql(meta_file).compile()


if __name__ == '__main__':
    # main('./my_complex_query.sql')
    typer.run(main)
