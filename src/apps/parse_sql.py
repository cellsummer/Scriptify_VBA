# This script is to parse the sql with placeholders
# The value of the placeholders will be in the top section
import re
import os
import typer


class MetaSql():
    '''
    The SQL assumes the following structure
    Header: Optional
    Body_CTE: Optional
    Body_Statements: Mandatory
    '''

    def __init__(self, meta_file):
        self.meta_file = meta_file
        self.header = []
        self.body = []
        self.body_cte = []
        self.body_statements = []
        self._header_start_identifier = '/*---'
        self._header_end_indentifier = '---*/'
        self._cte_identifier = '/* CTE */'
        self._statement_identifier = '/* Main Statements */'
        self._divider = '/* ========= Generated from SQL Parser =========*/\n'

        with open(self.meta_file, 'r') as f:
            self.lines = f.readlines()

        # remove starting and ending blank lines
        while str.strip(self.lines[0]) == '':
            self.lines.pop(0)

        while str.strip(self.lines[-1]) == '':
            self.lines.pop(-1)

    def _get_header_and_body(self, ):
        # populate self.header and self.body
        if self.lines[0].strip() != self._header_start_identifier:
            self.header = []
        else:
            self.lines.pop(0)
            while self.lines[0].strip() != self._header_end_indentifier:
                self.header.append(self.lines[0])
                self.lines.pop(0)

            # At end of headers
            self.lines.pop(0)
            if self.lines[0].strip() != self._cte_identifier:
                self.body_cte = []
            else:
                self.lines.pop(0)
                while self.lines[0].strip() != self._statement_identifier:
                    self.body_cte.append(self.lines[0])
                    self.lines.pop(0)

                # The rest are body_statements
                self.lines.pop(0)
                statement = ''
                while len(self.lines) > 0:
                    statement += self.lines[0]
                    if ';' in self.lines[0]:
                        self.body_statements.append(statement)
                        statement = ''
                    self.lines.pop(0)

        return

    def _replace_placeholders(self,):
        # read header to a dictionary {var_key: var_value}
        vars = {}
        for line in self.header:
            kvp = line.split(":")
            vars[kvp[0].strip()] = kvp[1].strip()

        # loop through all lines and replace placeholders in cte
        for i, line in enumerate(self.body_cte):
            # keys are {{ var }}
            placehoders = re.findall(r'\{\{.*\}\}', line)
            for p in placehoders:
                # stip out the double bracelets
                var_key = p[2:-2].strip()
                line = line.replace(p, vars[var_key])
            self.body_cte[i] = line

        # loop through all lines and replace placeholders in statements
        for i, line in enumerate(self.body_statements):
            # keys are {{ var }}
            placehoders = re.findall(r'\{\{.*\}\}', line)
            for p in placehoders:
                # stip out the double bracelets
                var_key = p[2:-2].strip()
                line = line.replace(p, vars[var_key])
            self.body_statements[i] = line

        return

    def _duplicate_cte(self,):
        # duplicate cte for each statement
        self.body.append(self._divider)
        for statement in self.body_statements:
            for line_cte in self.body_cte:
                self.body.append(line_cte)
            self.body.append(statement)
            self.body.append(self._divider)
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
        self._duplicate_cte()
        self._save_compiled_sql()


def main(meta_file):
    MetaSql(meta_file).compile()


if __name__ == '__main__':
    # main('./my_complex_query.sql')
    typer.run(main)
