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

    parser = argparse.ArgumentParser(description="vsub--a tool from venus group that helps you execute command without knowing the pbs usage."
                                                 "The behavior is the totally same as executing shell in your local machine."
                                                 "For example, the tool will execute shell in the current directory in the target node.")


    # parser.
    parser.add_argument('--shell', '-S', help='the shell to execute', type=str, required=True, dest='shell')
    parser.add_argument('--name', metavar='XXX', help='The output file name returned by pbs system', type=str, dest='name', default="")
    parser.add_argument('--node', '-N', help='the node you want to run program', type=int, required=True, dest='node')#sist-gpu0x
    parser.add_argument('--dest', '-D', help="the directory of the output file. Only Support absolute path.", type=str, default="./", dest='dest')
    parser.add_argument('--queue', '-Q', metavar='sist-xxx',
                        help='sist-xx. The queue name which could be fould by qstat command(Default:sist-hexm)', type=str,
                        default='sist-hexm', dest='queue')
    parser.add_argument('--stdout', help='Whether to print the output to standard output', type=bool, default=True, dest='stdout_flag')
    parser.add_argument('--cmd', action='store_true', help="Is the shell just a command?", dest='cmd_flag')
    # parser.add_argument('--gpu', '-G', help='specify the gpu', type=list, default=[0,1,2,3], dest='gpu')

    args = parser.parse_args()

    # import ipdb;ipdb.set_trace()
    # Convert the input format

    # args.shell
    args.shell = os.path.expanduser(args.shell)

    # args.name
    if args.name is "":
        args.name = os.path.basename(args.shell).split('.')[0]

    # args.dest
    args.dest = os.path.expanduser(args.dest)
    # TODO: Check the correctness of arguments
    assert args.cmd_flag or os.path.exists(args.shell), "the shell doesn't exist"
    # import ipdb;
    # ipdb.set_trace()
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
    file.write("#PBS -l nodes={}\n".format(node_name))

    cur_path = os.getcwd() #save a bak for recovery

    os.chdir(args.dest)
    dest_dir = os.getcwd()
    out_file = os.path.join(dest_dir, "{}.out".format(args.name))
    err_file = os.path.join(dest_dir, "{}.err".format(args.name))

    file.write("#PBS -o {}\n".format(out_file))
    file.write("#PBS -e {}\n".format(err_file))

    os.chdir(cur_path)
    file.write("cd {}\n".format(cur_path))


def generate_pb_file(args):
    # open file

    tmp_file_name = 'vsub.tmp' + str(random.randint(0,999))
    tmp_file = open(tmp_file_name,'w')

    if args.cmd_flag is True:
        tmp_file.write("#!/bin/bash\n")
        write_pbs_config_into_shell(tmp_file, args)
        tmp_file.write(args.shell + '\n')
    else:
        shell = open(args.shell, 'r')
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
    # import ipdb;ipdb.set_trace()
    cmd = ['qsub', tmpshell_name]
    subprocess.run(cmd, stdout=subprocess.PIPE)
    if stdout_flag:
        # import ipdb;ipdb.set_trace()
        try:
            file = prog_name + '.out'
            while not os.path.exists(file):
                time.sleep(0.2)
            subprocess.call(['less', '-f', file])
        except KeyboardInterrupt:
            subprocess.call(['rm', tmpshell_name])
            return
    subprocess.call(['rm', tmpshell_name])


if __name__ == '__main__':
    args = parse_args()
    pb_file_name = generate_pb_file(args)
    execute_pb_file(pb_file_name, args.name, args.stdout_flag)



# TODO: stdout_flag









