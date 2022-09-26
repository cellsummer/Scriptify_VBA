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

    def _get_header_and_body(self, ):
        # populate self.header and self.body
        if self.lines[0].strip() != '---':
            self.header = []
            self.body = self.lines
            return

        for i in range(1, len(self.lines)):
            if self.lines[i].strip() == '---':
                break
            else:
                self.header.append(self.lines[i])

        self.body = self.lines[i+1:]

        return

    def _replace_placeholders(self,):
        # read header to a dictionary {var_key: var_value}
        vars = {}
        for line in self.header:
            kvp = line.split(":")
            vars[kvp[0].strip()] = kvp[1].strip()

        # loop through all lines and replace placeholders
        for i, line in enumerate(self.body):
            # keys are {{ var }}
            placehoders = re.findall(r'\{\{.*\}\}', line)
            for p in placehoders:
                # stip out the double bracelets
                var_key = p[2:-2].strip()
                line = line.replace(p, vars[var_key])
            self.body[i] = line

        return

    def _save_compiled_sql(self, ):
        fname = os.path.splitext(self.meta_file)[0]
        fext = os.path.splitext(self.meta_file)[1]

        compiled_fname = fname + "_compiled"

        with open(f'{compiled_fname}{fext}', 'w') as f:
            f.writelines(self.body)

        return

    def compile(self, ):
        self._get_header_and_body()
        self._replace_placeholders()
        self._save_compiled_sql()


def main(meta_file):
    MetaSql(meta_file).compile()


if __name__ == '__main__':
    # main('./my_complex_query.sql')
    typer.run(main)
