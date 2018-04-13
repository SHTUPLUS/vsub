#!/usr/bin/env python

import subprocess
import argparse
import sys
import os
import random
import time

# Node, queue, gpu, output_file, error_file, name
def parse_args():
    #execute the shell directly

    parser = argparse.ArgumentParser(description="vsub--a tool from group venus that helps you execute command without knowing the pbs usage")


    # parser.
    parser.add_argument('--shell', '-S', help='the shell to execute', type=str, required=True, dest='shell')
    parser.add_argument('--name', metavar='XXX', help='The output file name returned by pbs system', type=str, dest='name', default="")
    parser.add_argument('--node', '-N', help='the node you want to run program', type=int, required=True, dest='node')#sist-gpu0x
    parser.add_argument('--queue', '-Q', metavar='sist-xxx',
                        help='sist-xx. The queue name which could be fould by qstat command', type=str,
                        default='sist-hexm', dest='queue')
    parser.add_argument('--stdout', help='Whether to print the output to standard output', type=bool, default=True, dest='stdout_flag')
    # parser.add_argument('--gpu', '-G', help='specify the gpu', type=list, default=[0,1,2,3], dest='gpu')

    args = parser.parse_args()
    if args.name is "":
        args.name = os.path.basename(args.shell).split('.')[0]
    # TODO: Check the correctness of arguments
    assert os.path.exists(args.shell), "the shell doesn't exist"

    return args

def write_pbs_config_into_shell(file, args):
    """
    parameters
    -----------
    file: the file object returned by open function
    args: parsed by argparser. Including name, queue, node, gpu

    """
    file.write("#PBS -N {}\n".format(args.name))
    file.write("#PBS -q {}\n".format(args.queue))
    node_name = "sist-gpu{:02d}".format(args.node)
    file.write("#PBS -o {}.out\n".format(args.name))
    file.write("#PBS -e {}.err\n".format(args.name))

def generate_pb_file(args):
    # open file
    shell = open(args.shell, 'r')
    tmp_file_name = 'vsub.tmp' + str(random.randint(0,999))
    tmp_file = open(tmp_file_name,'w')

    HAVE_NON_BLANK_LINE = False
    FIRST_LINE_PATTERNS = ['/usr/bin/env', 'python', 'bash']
    for i, line in enumerate(shell):
        line = line.strip()
        if len(line)==0:
            continue # Remove blank lines
        if not HAVE_NON_BLANK_LINE:
            if line[0]!='#' or 'PBS ' in line:
                raise ValueError("First line should start with # and specify which interpreter you want to use")
            if not any([patt in line for patt in FIRST_LINE_PATTERNS]):
                raise Warning("There maybe something wrong in the first line.")
            tmp_file.write(line+'\n')
            write_pbs_config_into_shell(tmp_file, args)
            HAVE_NON_BLANK_LINE = True
        elif 'PBS' in line and '#' in line:
            continue
        else:
            tmp_file.write(line+'\n')

    shell.close()
    tmp_file.close()

    return tmp_file_name


def execute_pb_file(tmpshell_name, prog_name, stdout_flag):
    """
    :parameter
    ------------
    file: file name
    """
    cmd = ['qsub', tmpshell_name]
    subprocess.run(cmd, stdout=subprocess.PIPE)
    time.sleep(1)
    if stdout_flag:
        # import ipdb;ipdb.set_trace()
        try:
            subprocess.call(['less', '-f', prog_name+'.out'])
        except KeyboardInterrupt:
            subprocess.call(['rm', tmpshell_name])
            return
    subprocess.call(['rm', tmpshell_name])


if __name__ == '__main__':
    args = parse_args()
    pb_file_name = generate_pb_file(args)
    execute_pb_file(pb_file_name, args.name, args.stdout_flag)



# TODO: stdout_flag









