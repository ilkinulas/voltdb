# This file is part of VoltDB.

# Copyright (C) 2008-2016 VoltDB Inc.
#
# This file contains original code and/or modifications of original code.
# Any modifications made by VoltDB Inc. are licensed under the following
# terms and conditions:
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.

from voltcli import utility

@VOLT.Command(
    bundles = VOLT.AdminBundle(),
    description = 'Update the Log4J configuration.',
    arguments = (
        VOLT.PathArgument('log4j_xml_path', 'the Log4J configuration file', exists=True),
    )
)
def log4j(runner):
    log4j_file = utility.File(runner.opts.log4j_xml_path)
    try:
        log4j_file.open()
        xml_text = log4j_file.read()
        response = runner.call_proc(
            '@UpdateLogging',
            [VOLT.FastSerializer.VOLTTYPE_STRING],
            [xml_text])
        print response
    finally:
        log4j_file.close()
